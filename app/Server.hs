{-# LANGUAGE RecordWildCards, LambdaCase #-}
module Server where

import Control.Concurrent
import Control.Monad
import Control.Monad.STM
import qualified STMContainers.Map as M
import qualified ListT
import Control.Exception
import Network.Socket hiding (recv)
import Network.Socket.ByteString (recv, sendAll)
import qualified Network.Socket.ByteString as L
import qualified Data.ByteString.Lazy.Char8 as BL
import qualified Data.ByteString.Char8 as B
import qualified Data.Serialize as S
import qualified Data.Aeson as JSON
import System.Console.Chalk
import System.Timeout
import System.Exit

import RPC
import RPC.Socket

-- | A type for mutable state.
data MutState = MutState
  { hash :: M.Map String String }

initialState :: IO MutState
initialState = MutState <$> M.newIO

-- | Runs the command with side effects and returns the response that is to be
-- sent back to the client.
runCommand :: (Int, ServerCommand) -- ^ A pair of the request ID and a command.
           -> MutState -- ^ A reference to the mutable state.
           -> IO Response
runCommand (i, cmd) MutState{..} =
  case cmd of
    Print s -> do
      putStrLn $ green "Printing:" ++ " " ++ s
      return $ Executed i Ok
    Get k -> do
      get <- atomically $ M.lookup k hash
      case get of
        Just v -> do
          putStrLn $ green $ "Got \"" ++ k ++ "\""
          return $ GetResponse i Ok v
        Nothing -> do
          putStrLn $ red $ "Couldn't get \"" ++ k ++ "\""
          return $ GetResponse i NotFound ""
    Set k v -> do
      atomically $ M.insert v k hash
      putStrLn $ green $ "Set \"" ++ k ++ "\" to \"" ++ v ++ "\""
      return $ Executed i Ok
    QueryAllKeys -> do
      kvs <- atomically $ ListT.toList $ M.stream hash
      putStrLn $ green "Returned all keys"
      return $ KeysResponse i Ok (map fst kvs)

-- | Receives messages, decodes and runs the content if necessary, and returns
-- the response. Should be run after you accepted a connection.
runConn :: (Socket, SockAddr) -> MutState -> IO ()
runConn (sock, sockAddr) st = do
  timeoutAct (recvWithLen sock) (putStrLn $ red "Timeout when receiving") $
    \cmdMsg -> case S.decode cmdMsg :: Either String (Int, ServerCommand) of
      Left e ->
        putStrLn $ red "Couldn't parse the message received because " ++ e
      Right (i, cmd) -> do
        response <- runCommand (i, cmd) st
        timeoutAct (sendWithLen sock (BL.toStrict (JSON.encode response)))
                   (putStrLn $ red "Timeout when sending")
                   return
  close sock

sendHeartbeat :: HostName -> UUIDString -> IO ()
sendHeartbeat server uuid = do
  attempt <- findAndConnectOpenPort undefined $ map show [39000..39010]
  case attempt of
    Nothing ->
      die $ bgRed $ "Heartbeat: Couldn't connect to ports 39000 to 39010 on " ++ server
    Just (sock, sockAddr) -> do
      timeoutDie
        (sendWithLen sock (S.encode (Heartbeat undefined undefined)))
        (red "Timeout error when sending")
      r <- timeoutDie (recvWithLen sock) (red "Timeout error when receiving")
      close sock
      case JSON.decode (BL.fromStrict r) of
        Nothing -> do
          putStrLn $ bgRed "Couldn't parse heartbeat response"
          exitFailure
        Just (Executed _ Ok) ->
          putStrLn "Heartbeat sent"
        Just _ -> do
          putStrLn $ bgRed "Heartbeat: View leader rejects the server ID"
          exitFailure

-- | The main loop that keeps accepting more connections.
-- Should be revised for concurrency.
loop :: Socket -> MutState -> IO ()
loop sock st = do
  conn <- accept sock
  forkIO (runConn conn st)
  loop sock st

main :: IO ()
main = do
    uuid <- newUUID
    attempt <- findAndListenOpenPort $ map show [38000..38010]
    case attempt of
      Nothing -> die $ bgRed "Couldn't bind ports 38000 to 38010"
      Just (sock, sockAddr) -> do
        setInterval (sendHeartbeat undefined uuid >> pure True) 5000000 -- every 5 sec
        st <- initialState
        loop sock st
        close sock
