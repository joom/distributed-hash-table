{-# LANGUAGE RecordWildCards #-}
module Server where

import Control.Monad
import Control.Exception
import Network.Socket hiding (recv)
import Network.Socket.ByteString (recv, sendAll)
import qualified Network.Socket.ByteString as L
import qualified Data.ByteString.Lazy.Char8 as BL
import qualified Data.ByteString.Char8 as B
import qualified Data.Serialize as S
import qualified Data.Aeson as JSON
import qualified Data.HashTable.IO as H
import qualified Data.IORef as R
import System.Console.Chalk
import System.Timeout
import System.Exit

import RPC

type HashTable = H.BasicHashTable String String

data MutState = MutState
  { hash :: HashTable
  , i    :: R.IORef Int }

initialState :: IO MutState
initialState = do
    hash <- H.new
    i <- R.newIORef 0
    return $ MutState hash i

getSocket :: ServiceName -> IO (Socket, SockAddr)
getSocket serv = do
    addrInfos <- getAddrInfo
                  (Just (defaultHints {addrFlags = [AI_PASSIVE]}))
                  Nothing (Just serv)
    let serverAddr = head addrInfos
    sock <- socket (addrFamily serverAddr) Stream defaultProtocol
    return (sock, addrAddress serverAddr)

findAndListenOpenPort :: IO (Maybe (Socket, SockAddr))
findAndListenOpenPort = foldM (\success port ->
    case success of
      Just _ -> return success -- we already have a successful connection, don't try
      _ -> do
        (sock, sockAddr) <- getSocket (show port)
        attempt <- timeout 5000000 (try (bind sock sockAddr))
        case (attempt :: Maybe (Either IOException ())) of
          Nothing -> do
            close sock
            putStrLn $ red ("Timeout error with binding the port " ++ show port)
            return Nothing
          Just (Left e) -> do
            close sock
            putStrLn $ red ("Couldn't bind the port " ++ show port)
                    ++ " because " ++ show e
            return Nothing
          Just (Right ()) -> do
            listen sock 1
            putStrLn $ green ("Listening to " ++ show port)
            return $ Just (sock, sockAddr)
  ) Nothing [38000..38010]


runCommand :: Command -> MutState -> IO Response
runCommand cmd MutState{..} = do
  R.modifyIORef i (+1)
  i' <- R.readIORef i
  let i'' = "r" ++ show i'
  case cmd of
    Print s -> do
      putStrLn $ green "Printing:" ++ " " ++ s
      return $ Executed i'' Ok
    Get k -> do
      get <- H.lookup hash k
      case get of
        Just v -> do
          putStrLn $ green $ "Got \"" ++ k ++ "\""
          return $ GetResponse i'' Ok v
        Nothing -> do
          putStrLn $ red $ "Couldn't get \"" ++ k ++ "\""
          return $ GetResponse i'' NotFound ""
    Set k v -> do
      H.insert hash k v
      putStrLn $ green $ "Set \"" ++ k ++ "\" to \"" ++ v ++ "\""
      return $ Executed i'' Ok
    QueryAllKeys -> do
      kvs <- H.toList hash
      putStrLn $ green "Returned all keys"
      return $ KeysResponse i'' Ok (map fst kvs)

runConn :: (Socket, SockAddr) -> MutState -> IO ()
runConn (sock, sockAddr) st = do
  lenStr <- recv sock msgLenBytes
  let lenInt = read (B.unpack lenStr) :: Int
  cmdMsg <- recv sock lenInt
  case S.decode cmdMsg :: Either String Command of
    Left e ->
     putStrLn $ red "Couldn't parse the command received from the client because " ++ e
    Right cmd -> do
      response <- runCommand cmd st
      let msg = JSON.encode response
      let len = fromIntegral $ BL.length msg
      sendAll sock (B.pack (intWithCompleteBytes len msgLenBytes))
      sendAll sock (BL.toStrict msg)
  close sock

loop :: Socket -> MutState -> IO ()
loop sock st = do
  conn <- accept sock
  runConn conn st
  loop sock st

main :: IO ()
main = do
    attempt <- findAndListenOpenPort
    case attempt of
      Nothing -> die $ bgRed "Couldn't bind ports 38000 to 38010"
      Just (sock, sockAddr) -> do
        st <- initialState
        loop sock st
        close sock
