{-# LANGUAGE RecordWildCards, LambdaCase #-}

module Client where

import Control.Concurrent (threadDelay)
import Control.Monad
import Control.Monad.STM
import Control.Concurrent.STM.TVar
import Control.Exception
import qualified Options.Applicative as A
import Options.Applicative (Parser, (<>))
import Network.Socket hiding (send, sendTo, recv, recvFrom)
import Network.Socket.ByteString
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Lazy.Char8 as BL
import qualified Data.Serialize as S
import qualified Data.Aeson as JSON
import Data.Hashable
import Data.Maybe
import Data.Either
import Data.List
import System.Console.Chalk
import System.Timeout
import System.Exit

import RPC
import RPC.Socket

-- | A type for mutable state.
data MutState = MutState
  { nextRequestId :: TVar Int
  }

initialState :: IO MutState
initialState = MutState <$> newTVarIO 0

data CommandTypes =
    ServerCmd ServerCommand
  | ViewCmd ViewLeaderCommand
  deriving (Show)

data Options = Options
  { serverAddr     :: HostName
  , viewLeaderAddr :: HostName
  , cmd            :: CommandTypes
  } deriving (Show)

data RequestError =
    CouldNotConnect [ServiceName]
  | SendingTimeout
  | ReceivingTimeout
  | InvalidResponse
  deriving (Show)

getResponse :: Options
            -> MutState
            -> CommandTypes -- ^ The command to send. Changes the default values for host and port.
            -> Maybe HostName -- ^ The host to try to connect, overrides default if it is 'Just'.
            -> Maybe [ServiceName] -- ^ The ports to try to connect, overrides default if it is 'Just'.
            -> IO (Either RequestError Response)
getResponse opt@Options{..} st@MutState{..} cmd' host ports = do
    i <- atomically $ readTVar nextRequestId
    atomically $ modifyTVar' nextRequestId (+1)
    let commandPorts = pick (fromMaybe (map show [38000..38010]) ports) (map show [39000..39010])
    let addr         = fromMaybe (pick serverAddr viewLeaderAddr) host
    let encoded      = pick' (\c -> S.encode (i, c)) (\c -> S.encode (i, c))
    attempt <- findAndConnectOpenPort addr commandPorts
    case attempt of
      Nothing -> return $ Left $ CouldNotConnect commandPorts
      Just (sock, sockAddr) -> do
        either <- timeoutAct
          (sendWithLen sock encoded)
          (return $ Left SendingTimeout) $ \() ->
            timeoutAct (recvWithLen sock) (return $ Left ReceivingTimeout) $ \r ->
              case JSON.decode (BL.fromStrict r) :: Maybe Response of
                Just res -> return $ Right res
                _ -> return $ Left InvalidResponse
        close sock
        return either
  where
    pick  a b = case cmd' of {ServerCmd _ -> a   ; ViewCmd _ -> b}
    pick' f g = case cmd' of {ServerCmd c -> f c ; ViewCmd c -> g c}

getServers :: Options -> MutState -> IO (Maybe (Int, [(UUIDString, String)]))
getServers opt@Options{..} st@MutState{..} = do
  res <- getResponse opt st (ViewCmd QueryServers) Nothing Nothing
  case res of
    Left err -> do
      putStrLn $ bgRed $ show err
      return Nothing
    Right (QueryServersResponse _ epoch servers) -> do
      return $ Just (epoch, servers)
    Right _ -> do
      putStrLn $ bgRed "Received incorrect kind of response"
      return Nothing

-- | Runs the program once it receives a successful parse of the input given.
run :: Options -- ^ Command line options.
    -> MutState
    -> IO ()
run opt@Options{..} st@MutState{..} =
  case cmd of
    ServerCmd c@GetR{..} ->
      getServers opt st >>= \case
        Nothing -> die $ bgRed "Couldn't get active servers from view leader"
        Just (viewEpoch, servers) -> do
          let buckets = bucketAllocator k servers fst
          result <- foldM (\success (uuidStr, addr) ->
            case success of
              Just _ -> return success -- we already got it successfully
              Nothing -> do
                let (host, port) = addrStringPair addr
                getResponse opt st (ServerCmd $ c {epochInput = viewEpoch}) (Just host) (Just [port]) >>= \case
                  Left _ -> return Nothing
                  Right res@GetResponse{..} ->
                    return $ Just res
                  Right _ -> return Nothing
            ) Nothing buckets
          case result of
            Nothing -> die $ bgRed "All servers failed"
            Just res -> print res
    ServerCmd c@SetRVote{..} ->
      getServers opt st >>= \case
        Nothing -> die $ bgRed "Couldn't get active servers from view leader"
        Just (viewEpoch, servers) -> do -- Two phase commit
          let buckets = bucketAllocator k servers fst
          responsesM <- forM buckets $ \(uuidStr, addrStr) -> do
            let (host, port) = addrStringPair addrStr
            getResponse opt st (ServerCmd $ c {epochInput = viewEpoch}) (Just host) (Just [port]) -- Phase one request
          let validResponses = rights responsesM
          if all isRight responsesM && all ((== Ok) . status) validResponses
          then do -- Finalize commit
            acknowledgmentsM <- forM (zip buckets validResponses) $ \((uuidStr, addrStr), res) ->
              case res of
                SetResponseR i Ok ep commitId -> do
                  let (host, port) = addrStringPair addrStr
                  getResponse opt st (ServerCmd (SetRCommit k commitId)) (Just host) (Just [port]) >>= \case
                    Right res@(Executed _ Ok) -> do
                      putStrLn $ green $ "Successfully finalized commit " ++ show commitId
                                 ++ " for the key \"" ++ k ++ "\" on " ++ addrStr
                      return $ Just res
                    Right _ -> do
                      putStrLn $ red $ "Couldn't finalize commit " ++ show commitId
                                 ++ " for key \"" ++ k ++ "\" on " ++ addrStr
                      return Nothing
                    Left err -> do
                      putStrLn $ red $ "Couldn't get response from " ++ addrStr
                                 ++ " during commit finalization for the key \""
                                 ++ k ++ " with id " ++ show commitId
                                 ++ " because of " ++ show err
                      return Nothing
                _ -> do
                  putStrLn $ bgRed $ "Unreachable case"
                  return Nothing
            case sequence acknowledgmentsM of
              Nothing -> die $ bgRed $ "Commit failed"
              Just acknowledgments -> do
                putStrLn $ green $ "Commit succeeded: " ++ show acknowledgments
                exitSuccess
          else do -- Cancel commit
            let bucketsToCancel = filter (isRight . snd) (zip buckets responsesM)
            cancelAll <- forM bucketsToCancel $ \((uuidStr, addrStr), res) ->
              case res of
                Right (SetResponseR i Ok ep commitId) -> do
                  let (host, port) = addrStringPair addrStr
                  getResponse opt st (ServerCmd (SetRCancel k commitId)) (Just host) (Just [port]) >>= \case
                    Right (Executed _ Ok) ->
                      putStrLn $ yellow $ "Successfully cancelled commit " ++ show commitId
                                 ++ " for the key \"" ++ k ++ "\" on " ++ addrStr
                    _ ->
                      putStrLn $ red $ "Couldn't cancel commit " ++ show commitId
                                 ++ " for key \"" ++ k ++ "\" on " ++ addrStr
                _ -> putStrLn $ yellow $ "There is no commit to cancel for the key \"" ++ k
                                ++ "\" on " ++ addrStr
            die $ bgRed "Commit failed"
    ServerCmd QueryAllKeys -> -- connects to all active servers and fetches all keys
      getServers opt st >>= \case
        Nothing -> die $ bgRed "Couldn't get active servers from view leader"
        Just (viewEpoch, servers) -> do
          keysLists <- forM servers $ \server@(uuidString, addrStr) -> do
            let (host, port) = addrStringPair addrStr
            getResponse opt st cmd (Just host) (Just [port]) >>= \case
              Left err -> do
                putStrLn $ red "Failed to get keys from the server " ++ addrStr ++ " because of " ++ show err
                return []
              Right KeysResponse{..} -> return keys
              Right _ -> do
                putStrLn $ red "The server " ++ addrStr ++ " returned an invalid response instead of keys"
                return []
          let allKeys = sort $ nub $ concat keysLists
          print allKeys
    ServerCmd _ -> error $ bgRed "Unimplemented on purpose"
    ViewCmd _ ->
      getResponse opt st cmd Nothing Nothing >>= \case
        Left err -> die $ bgRed "View leader command failed because of " ++ show err
        Right c@(Executed _ Retry) -> do
          print c
          putStrLn $ yellow "Waiting for 5 sec"
          threadDelay 5000000 -- wait 5 sec
          run opt st
        Right c -> print c >> exitSuccess

-- | Parser for a ServerCommand, i.e. the procedures available in the system.
commandParser :: Parser CommandTypes
commandParser = A.subparser $
     A.command "getr"
      (A.info
        (ServerCmd <$>
          (GetR <$> A.strArgument (A.metavar "KEY" <> A.help "Key to get")
                <*> pure 0)) -- has to be changed
        (A.progDesc "Get a value from the distributed key store."))
  <> A.command "setr"
      (A.info
        (ServerCmd <$>
          (SetRVote <$> A.strArgument (A.metavar "KEY" <> A.help "Key to set")
                    <*> A.strArgument (A.metavar "VAL" <> A.help "Value to set")
                    <*> pure 0)) -- has to be changed
        (A.progDesc "Set a value in the distributed key store."))
  <> A.command "query_all_keys"
      (A.info
        (pure $ ServerCmd QueryAllKeys)
        (A.progDesc "Get all the keys from the key store."))
  <> A.command "query_servers"
      (A.info
        (pure $ ViewCmd QueryServers)
        (A.progDesc "Get all the keys from the key store."))
  <> A.command "lock_get"
      (A.info
        (ViewCmd <$>
          (LockGet
            <$> A.strArgument (A.metavar "NAME" <> A.help "Lock name")
            <*> A.strArgument (A.metavar "ID" <> A.help "Requester ID.")))
        (A.progDesc "Get a lock from the view leader."))
  <> A.command "lock_release"
      (A.info
        (ViewCmd <$>
          (LockRelease
            <$> A.strArgument (A.metavar "NAME" <> A.help "Lock name")
            <*> A.strArgument (A.metavar "ID" <> A.help "Requester ID.")))
        (A.progDesc "Release a lock from the view leader."))

-- | Parser for the optional parameters of the client.
optionsParser :: Parser Options
optionsParser = Options
  <$> A.strOption
      ( A.long "server"
     <> A.short 's'
     <> A.metavar "SERVERADDR"
     <> A.help "Address of the server to connect"
     <> A.value "localhost" )
  <*> A.strOption
      ( A.long "viewleader"
     <> A.short 'l'
     <> A.metavar "VIEWLEADERADDR"
     <> A.help "Address of the view leader to connect"
     <> A.value "localhost" )
  <*> commandParser

main :: IO ()
main = do
    st <- initialState
    opt <- A.execParser opts
    run opt st
  where
    opts = A.info (A.helper <*> optionsParser)
      ( A.fullDesc
     <> A.progDesc "Connect to the server at HOST with the given command"
     <> A.header "client for an RPC implementation with locks and a view leader" )
