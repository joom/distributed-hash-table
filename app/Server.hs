{-# LANGUAGE RecordWildCards #-}
module Server where

import Control.Concurrent
import Control.Monad
import Control.Monad.STM
import Control.Concurrent.STM.TVar
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
import qualified Options.Applicative as A
import Options.Applicative (Parser, (<>))

import RPC
import RPC.Socket

data Options = Options
  { viewLeaderAddr :: HostName
  } deriving (Show)

-- | A type for mutable state.
data MutState = MutState
  { keyStore   :: M.Map String String
  , heartbeats :: TVar Int
  }

initialState :: IO MutState
initialState = MutState <$> M.newIO <*> newTVarIO 0

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
      get <- atomically $ M.lookup k keyStore
      case get of
        Just v -> do
          putStrLn $ green $ "Got \"" ++ k ++ "\""
          return $ GetResponse i Ok v
        Nothing -> do
          putStrLn $ red $ "Couldn't get \"" ++ k ++ "\""
          return $ GetResponse i NotFound ""
    Set k v -> do
      atomically $ M.insert v k keyStore
      putStrLn $ green $ "Set \"" ++ k ++ "\" to \"" ++ v ++ "\""
      return $ Executed i Ok
    QueryAllKeys -> do
      kvs <- atomically $ ListT.toList $ M.stream keyStore
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

sendHeartbeat :: Options -- ^ Command line options.
              -> SockAddr -- ^ The socket address of the listening server.
              -> UUIDString -- ^ UUID created at the beginning of the program.
              -> MutState -- ^ A reference to the mutable state.
              -> IO ()
sendHeartbeat Options{..} sockAddr uuid MutState{..} = do
  attempt <- findAndConnectOpenPort viewLeaderAddr $ map show [39000..39010]
  i <- atomically $ modifyTVar' heartbeats (+1) >> readTVar heartbeats
  case attempt of
    Nothing ->
      die $ bgRed $ "Heartbeat: Couldn't connect to ports 39000 to 39010 on " ++ viewLeaderAddr
    Just (sock, sockAddr) -> do
      timeoutDie
        (sendWithLen sock (S.encode (i, Heartbeat uuid (show sockAddr))))
        (red "Timeout error when sending heartbeat request")
      r <- timeoutDie (recvWithLen sock)
            (red "Timeout error when receiving heartbeat response")
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
loop :: Socket -> MutState -> IO ()
loop sock st = do
  conn <- accept sock
  forkIO (runConn conn st)
  loop sock st

run :: Options -> IO ()
run opt = do
    uuid <- newUUID
    attempt <- findAndListenOpenPort $ map show [38000..38010]
    case attempt of
      Nothing -> die $ bgRed "Couldn't bind ports 38000 to 38010"
      Just (sock, sockAddr) -> do
        st <- initialState
        setInterval (sendHeartbeat opt sockAddr uuid st >> pure True) 5000000 -- every 5 sec
        loop sock st
        close sock

-- | Parser for the optional parameters of the client.
optionsParser :: Parser Options
optionsParser = Options
  <$> A.strOption
      ( A.long "viewleader"
     <> A.short 'l'
     <> A.metavar "VIEWLEADERADDR"
     <> A.help "Address of the view leader to connect"
     <> A.value "localhost" )

main :: IO ()
main = A.execParser opts >>= run
  where
    opts = A.info (A.helper <*> optionsParser)
      ( A.fullDesc
     <> A.progDesc "Start the server"
     <> A.header "client for an RPC implementation with locks and a view leader" )
