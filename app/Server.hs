{-# LANGUAGE RecordWildCards, LambdaCase #-}
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

-- | A type for mutable state.
data MutState = MutState
  { hash :: HashTable }

initialState :: IO MutState
initialState = do
    hash <- H.new
    return $ MutState hash

-- | Starts a server socket with the given port.
getSocket :: ServiceName -> IO (Socket, SockAddr)
getSocket serv = do
    addrInfos <- getAddrInfo
                  (Just (defaultHints {addrFlags = [AI_PASSIVE]}))
                  Nothing (Just serv)
    let serverAddr = head addrInfos
    sock <- socket (addrFamily serverAddr) Stream defaultProtocol
    return (sock, addrAddress serverAddr)

-- | Tries to listen to a port from 38000 to 38010.
-- Returns Nothing if they all fail. Has side effect of printing log messages.
findAndListenOpenPort :: IO (Maybe (Socket, SockAddr))
findAndListenOpenPort = foldM (\success port ->
    case success of
      Just _ -> return success -- we already have a successful conn, don't try
      _ -> do
        (sock, sockAddr) <- getSocket (show port)
        attempt <- timeout timeoutTime (try (bind sock sockAddr))
        case (attempt :: Maybe (Either IOException ())) of
          Nothing -> do
            close sock
            putStrLn $ red $ "Timeout error when binding port " ++ show port
            return Nothing
          Just (Left e) -> do
            close sock
            putStrLn $ red $ "Couldn't bind port "
                          ++ show port ++ " because " ++ show e
            return Nothing
          Just (Right ()) -> do
            listen sock 1
            putStrLn $ green $ "Listening to " ++ show port
            return $ Just (sock, sockAddr)
  ) Nothing [38000..38010]

-- | Runs the command with side effects and returns the response that is to be
-- sent back to the client.
runCommand :: (Int, Command) -- ^ A pair of the request ID and a command.
           -> MutState -- ^ A reference to the mutable state.
           -> IO Response
runCommand (i, cmd) MutState{..} =
  case cmd of
    Print s -> do
      putStrLn $ green "Printing:" ++ " " ++ s
      return $ Executed i Ok
    Get k -> do
      get <- H.lookup hash k
      case get of
        Just v -> do
          putStrLn $ green $ "Got \"" ++ k ++ "\""
          return $ GetResponse i Ok v
        Nothing -> do
          putStrLn $ red $ "Couldn't get \"" ++ k ++ "\""
          return $ GetResponse i NotFound ""
    Set k v -> do
      H.insert hash k v
      putStrLn $ green $ "Set \"" ++ k ++ "\" to \"" ++ v ++ "\""
      return $ Executed i Ok
    QueryAllKeys -> do
      kvs <- H.toList hash
      putStrLn $ green "Returned all keys"
      return $ KeysResponse i Ok (map fst kvs)

-- | Receives messages, decodes and runs the content if necessary, and returns
-- the response. Should be run after you accepted a connection.
runConn :: (Socket, SockAddr) -> MutState -> IO ()
runConn (sock, sockAddr) st = do
  timeoutAct (recvWithLen sock) (putStrLn $ red "Timeout when receiving") $
    \cmdMsg -> case S.decode cmdMsg :: Either String (Int, Command) of
      Left e ->
        putStrLn $ red "Couldn't parse the message received because " ++ e
      Right (i, cmd) -> do
        response <- runCommand (i, cmd) st
        timeoutAct (sendWithLen sock (BL.toStrict (JSON.encode response)))
                   (putStrLn $ red "Timeout when sending")
                   return
  close sock

-- | The main loop that keeps accepting more connections.
-- Should be revised for concurrency.
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
