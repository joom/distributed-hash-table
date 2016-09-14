module Server where

import Control.Monad
import Control.Exception
import Network.Socket hiding (recv)
import Network.Socket.ByteString (recv, sendAll)
import qualified Network.Socket.ByteString as L
import qualified Data.ByteString.Lazy.Char8 as BL
import qualified Data.ByteString.Char8 as B
import qualified Data.Serialize as S
import qualified Data.Map
import qualified Data.Aeson as JSON
import qualified Data.HashTable.IO as H
import System.Console.Chalk
import System.Exit

import RPC.Language

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
        either <- try $ bind sock sockAddr
        case (either :: Either IOException ()) of
          Left e -> do
            close sock
            putStrLn $ red ("Couldn't bind the port " ++ show port)
                    ++ " because " ++ show e
            return Nothing
          Right () -> do
            listen sock 1
            putStrLn $ green ("Listening to " ++ show port)
            return $ Just (sock, sockAddr)
  ) Nothing [38000..38010]

type HashTable = H.BasicHashTable String String

runCommand :: Command -> HashTable -> IO Response
runCommand (Print s) hash = do
  putStrLn $ green "Printing:" ++ " " ++ s
  return $ Executed Ok
runCommand (Get k) hash = do
  get <- H.lookup hash k
  case get of
    Just v -> do
      putStrLn $ green $ "Got \"" ++ k ++ "\""
      return $ GetResponse Ok v
    Nothing -> do
      putStrLn $ red $ "Couldn't get \"" ++ k ++ "\""
      return $ GetResponse NotFound ""
runCommand (Set k v) hash = do
  H.insert hash k v
  putStrLn $ green $ "Set \"" ++ k ++ "\" to \"" ++ v ++ "\""
  return $ Executed Ok
runCommand QueryAllKeys hash = do
  kvs <- H.toList hash
  putStrLn $ green "Returned all keys"
  return $ KeysResponse Ok (map fst kvs)

runConn :: (Socket, SockAddr) -> HashTable -> IO ()
runConn (sock, sockAddr) hash = do
  lenStr <- recv sock msgLenBytes
  let lenInt = read (B.unpack lenStr) :: Int
  cmdMsg <- recv sock lenInt
  case S.decode cmdMsg :: Either String Command of
    Left e ->
     putStrLn $ red "Couldn't parse the command received from the client because " ++ e
    Right cmd -> do
      response <- runCommand cmd hash
      let msg = JSON.encode response
      let len = fromIntegral $ BL.length msg
      sendAll sock (B.pack (intWithCompleteBytes len msgLenBytes))
      sendAll sock (BL.toStrict msg)
  close sock

loop :: Socket -> HashTable -> IO ()
loop sock hash = do
  conn <- accept sock
  runConn conn hash
  loop sock hash

main :: IO ()
main = do
    attempt <- findAndListenOpenPort
    case attempt of
      Nothing -> die $ bgRed "Couldn't bind ports 38000 to 38010"
      Just (sock, sockAddr) -> do
        hash <- H.new
        loop sock hash
        close sock
