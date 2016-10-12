{-# LANGUAGE RecordWildCards #-}
module ViewLeader where

import Control.Concurrent
import Control.Monad
import Control.Monad.STM
import qualified STMContainers.Map as M
import Control.Exception
import Data.UnixTime
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
  { heartbeats :: M.Map UUIDString UnixTime
  , locks      :: M.Map String UUIDString
  }

initialState :: IO MutState
initialState = MutState <$> M.newIO <*> M.newIO

-- | Runs the command with side effects and returns the response that is to be
-- sent back to the client.
runCommand :: (Int, ViewLeaderCommand) -- ^ A pair of the request ID and a command.
           -> MutState -- ^ A reference to the mutable state.
           -> IO Response
runCommand (i, cmd) MutState{..} =
  case cmd of
    Heartbeat uuid port -> do
      now <- getUnixTime
      atomically $ do
        get <- M.lookup uuid heartbeats
        case get of
          Just lastHeartbeatTime -> do
            let expireTime = lastHeartbeatTime `addUnixDiffTime` secondsToUnixDiffTime 30
            if now < expireTime
              then do
                M.insert now uuid heartbeats
                return $ Executed i Ok
              else return $ Executed i Forbidden
          Nothing -> do
            M.insert now uuid heartbeats
            return $ Executed i Ok
    QueryServers -> undefined
    LockGet name cli -> atomically $ do
      get <- M.lookup name locks
      case get of
        Just uuid -> return $ Executed i Retry
        Nothing -> do
          M.insert cli name locks
          return $ Executed i Granted
    LockRelease name cli -> atomically $ do
      get <- M.lookup name locks
      case get of
        Just uuid ->
          if uuid == cli
          then do
            M.delete name locks
            return $ Executed i Ok
          else return $ Executed i Forbidden
        Nothing ->
          return $ Executed i Ok

-- | Receives messages, decodes and runs the content if necessary, and returns
-- the response. Should be run after you accepted a connection.
runConn :: (Socket, SockAddr) -> MutState -> IO ()
runConn (sock, sockAddr) st = do
  timeoutAct (recvWithLen sock) (putStrLn $ red "Timeout when receiving") $
    \cmdMsg -> case S.decode cmdMsg :: Either String (Int, ViewLeaderCommand) of
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
  forkIO (runConn conn st)
  loop sock st

main :: IO ()
main = do
    attempt <- findAndListenOpenPort $ map show [39000..39010]
    case attempt of
      Nothing -> die $ bgRed "Couldn't bind ports 39000 to 39010"
      Just (sock, sockAddr) -> do
        st <- initialState
        loop sock st
        close sock
