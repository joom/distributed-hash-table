{-# LANGUAGE RecordWildCards, LambdaCase #-}
module Server where

import Control.Concurrent
import Control.Monad
import Control.Monad.STM
import Control.Concurrent.STM.TVar
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Writer.Strict
import qualified STMContainers.Map as M
import qualified STMContainers.Multimap as MM
import qualified STMContainers.Set as Set
import qualified ListT
import Control.Exception
import Network.Socket hiding (recv)
import Network.Socket.ByteString (recv, sendAll)
import qualified Network.Socket.ByteString as L
import qualified Data.ByteString.Lazy.Char8 as BL
import qualified Data.ByteString.Char8 as B
import qualified Data.Serialize as S
import qualified Data.Aeson as JSON
import Data.Hashable
import Data.List
import Data.Maybe
import Data.Either
import System.Console.Chalk
import System.Timeout
import System.Exit
import qualified Options.Applicative as A
import Options.Applicative (Parser, (<>))

import RPC
import RPC.Socket

data Options = Options
  { viewLeaderAddr :: HostName
  , uuid           :: UUIDString
  } deriving (Show)

type Bucket = (UUIDString, AddrString)

-- | A type for mutable state.
data MutState = MutState
  { keyStore      :: M.Map String String
  -- ^ A map to keep track of what we are committed on.
  -- Holds a pair of commit id and a key.
  , keyCommit     :: M.Map String (CommitId, String)
  , nextCommitId  :: TVar CommitId
  , nextRequestId :: TVar Int
  , heartbeats    :: TVar Int
  -- ^ Keeps track of the epoch in the view leader.
  , epoch         :: TVar Int
  -- ^ Keeps track of the most recent active servers list, so that we don't
  -- have to make a request to the view leader every time we make a request.
  , activeServers :: TVar [Bucket]
  }

initialState :: IO MutState
initialState = MutState <$> M.newIO <*> M.newIO <*> newTVarIO 0 <*> newTVarIO 0
                        <*> newTVarIO 0 <*> newTVarIO 0 <*> newTVarIO []

-- | Runs the command with side effects and returns the response that is to be
-- sent back to the client.
runCommand :: (Int, ServerCommand) -- ^ A pair of the request ID and a command.
           -> Options
           -> MutState -- ^ A reference to the mutable state.
           -> IO Response
runCommand (i, cmd) opt st@MutState{..} =
  case cmd of
    Get k -> returnAndLog $ runWriterT $
      lift (atomically $ (,) <$> M.lookup k keyStore <*> M.lookup k keyCommit) >>= \case
        (_, Just (i', v')) -> do
          logger $ red $ "Getting \"" ++ k ++ "\" is forbidden"
          return $ GetResponse i Forbidden ""
        (Just v, Nothing) -> do
          logger $ green $ "Got \"" ++ k ++ "\""
          return $ GetResponse i Ok v
        (Nothing, Nothing) -> do
          logger $ red $ "Couldn't get \"" ++ k ++ "\""
          return $ GetResponse i NotFound ""
    GetR k epochInput -> do
      ep <- atomically $ readTVar epoch
      if ep /= epochInput
      then do
        putStrLn $ red $ show ep ++ " and " ++ show epochInput ++ " are different epochs"
        return $ GetResponse i Forbidden ""
      else runCommand (i, Get k) opt st
    SetRVote k v epochInput -> returnAndLog $ runWriterT $ do -- Phase one
      ep <- lift $ atomically $ readTVar epoch
      currentId <- lift $ atomically $ readTVar nextCommitId
      lift (atomically $ M.lookup k keyCommit) >>= \case
        Just (i, v) -> do
          logger $ red $ "Setting \"" ++ k ++ "\" to \"" ++ v ++ "\" is forbidden, because "
                         ++ show (currentId, v) ++ " is in the commits"
          return $ SetResponseR i Forbidden ep currentId
        Nothing -> do
          lift $ atomically $ do
            modifyTVar' nextCommitId (+1)
            M.insert (currentId, v) k keyCommit
          logger $ yellow $ "Starting commit \"" ++ k ++ "\" to \"" ++ show (currentId, v)
          return $ SetResponseR i Ok ep currentId
    SetRCommit{..} -> returnAndLog $ runWriterT $ -- Phase two confirmation
      lift (atomically $ M.lookup k keyCommit) >>= \case
          Nothing -> do
            logger $ yellow $ "Commit: No such commit for \"" ++ k ++ "\" with the id " ++ show commitId
            return $ Executed i NotFound
          Just (id, v) -> if id /= commitId
            then do
              logger $ red $ "Commit: Wrong id. The commit for \"" ++ k ++ "\" with the id "
                             ++ show id ++ ", not with " ++ show commitId
              return $ Executed i Forbidden
            else do
              lift $ atomically $ do
                M.delete k keyCommit
                M.insert v k keyStore
              logger $ green $ "Finalizing commit for \"" ++ k ++ "\" with the id " ++ show commitId
              return $ Executed i Ok
    SetRCancel{..} -> returnAndLog $ runWriterT $ -- Phase two cancellation
      lift (atomically $ M.lookup k keyCommit) >>= \case
          Nothing -> do -- This should never be reached normally.
            logger $ yellow $ "Cancel: No such commit for \"" ++ k ++ "\" with the id " ++ show commitId
            return $ Executed i NotFound
          Just (id, v) -> if id /= commitId
            then do
              logger $ red $ "Cancel: Wrong id. The commit for \"" ++ k ++ "\" with the id "
                             ++ show id ++ ", not with " ++ show commitId
              return $ Executed i Forbidden
            else do
              lift $ atomically $ M.delete k keyCommit
              logger $ green $ "Canceling commit for \"" ++ k ++ "\" with the id " ++ show commitId
              return $ Executed i Ok
    QueryAllKeys -> returnAndLog $ runWriterT $ do
      kvs <- lift $ atomically $ ListT.toList $ M.stream keyStore
      logger $ green "Returned all keys"
      return $ KeysResponse i Ok (map fst kvs)
    Rebalance kvs epochInput -> do
      putStrLn $ magenta "Rebalancing request for keys and values " ++ show kvs
      atomically $ forM_ kvs $ \kv@(k, v) ->
        M.insert v k keyStore
      return $ Executed i Ok

-- | Receives messages, decodes and runs the content if necessary, and returns
-- the response. Should be run after you accepted a connection.
runConn :: (Socket, SockAddr) -> Options -> MutState -> IO ()
runConn (sock, sockAddr) opt st = do
  timeoutAct (recvWithLen sock) (putStrLn $ red "Timeout when receiving") $
    \cmdMsg -> case S.decode cmdMsg :: Either String (Int, ServerCommand) of
      Left e ->
        putStrLn $ red "Couldn't parse the message received because " ++ e
      Right (i, cmd) -> do
        response <- runCommand (i, cmd) opt st
        timeoutAct (sendWithLen sock (BL.toStrict (JSON.encode response)))
                   (putStrLn $ red "Timeout when sending")
                   return
  close sock

sendHeartbeat :: Options -- ^ Command line options.
              -> MutState -- ^ A reference to the mutable state.
              -> AddrString -- ^ The socket address of the listening server.
              -> IO ()
sendHeartbeat opt@Options{..} st@MutState{..} addrStr = do
  i <- atomically $ modifyTVar' heartbeats (+1) >> readTVar heartbeats
  findAndConnectOpenPort viewLeaderAddr (map show [39000..39010])  >>= \case
    Nothing ->
      die $ bgRed $ "Heartbeat: Couldn't connect to ports 39000 to 39010 on " ++ viewLeaderAddr
    Just (sock, sockAddr) -> do
      timeoutDie
        (sendWithLen sock (S.encode (i, Heartbeat uuid addrStr)))
        (red "Timeout error when sending heartbeat request")
      r <- timeoutDie (recvWithLen sock)
             (red "Timeout error when receiving heartbeat response")
      close sock
      case JSON.decode (BL.fromStrict r) of
        Just (HeartbeatResponse _ Ok newEpoch) -> do
          oldEpoch <- atomically $ swapTVar epoch newEpoch
          when (oldEpoch /= newEpoch) $ updateActiveServers opt st
          putStrLn "Heartbeat sent"
        Just _ -> do
          putStrLn $ bgRed "Heartbeat: View leader rejects the server ID"
          exitFailure
        Nothing -> do
          putStrLn $ bgRed "Couldn't parse heartbeat response"
          exitFailure

updateActiveServers :: Options -> MutState -> IO ()
updateActiveServers opt@Options{..} st@MutState{..} = do
  putStrLn $ yellow "Updating active servers list"
  attempt <- findAndConnectOpenPort viewLeaderAddr $ map show [39000..39010]
  i <- atomically $ readTVar heartbeats
  case attempt of
    Nothing ->
      die $ bgRed $ "Active servers: Couldn't connect to ports 39000 to 39010 on " ++ viewLeaderAddr
    Just (sock, sockAddr) -> do
      timeoutDie
        (sendWithLen sock (S.encode (i, QueryServers)))
        (red "Timeout error when sending query servers request")
      r <- timeoutDie (recvWithLen sock)
            (red "Timeout error when receiving query servers response")
      close sock
      case JSON.decode (BL.fromStrict r) of
        Just (QueryServersResponse _ newEpoch newActiveServers) -> do
          oldActiveServers <- atomically $ readTVar activeServers
          atomically $ do
            writeTVar epoch newEpoch
            writeTVar activeServers newActiveServers
          putStrLn $ green "Current list of active servers updated"
          rebalanceKeys opt st oldActiveServers
        Just _ ->
          putStrLn $ bgRed "Wrong kind of response for query servers"
        Nothing ->
          putStrLn $ bgRed "Couldn't parse query servers response"

type KV = (String, String)

-- ^ This can be written as a pure function.
collectUnderBuckets :: [(KV, [Bucket])] -> STM [(Bucket, [KV])]
collectUnderBuckets pairs = do
  bucketToKVMap <- MM.new :: STM (MM.Multimap Bucket KV)
  forM_ pairs $ \(kv, newBuckets) ->
    forM_ newBuckets $ \bucket -> MM.insert kv bucket bucketToKVMap
  buckets <- ListT.toList (MM.streamKeys bucketToKVMap)
  bucketsWithKVSets <- mapM (\bucket -> (,) <$> pure bucket <*>
                          (fromJust <$> MM.lookupByKey bucket bucketToKVMap)) buckets
  MM.deleteAll bucketToKVMap
  mapM (\(bucket, kvSet) ->
      (,) <$> pure bucket <*> ListT.toList (Set.stream kvSet)
    ) bucketsWithKVSets

-- ^ Needs serious refactoring.
rebalanceKeys :: Options
              -> MutState
              -> [Bucket] -- ^ Old version of active servers before update.
              -> IO ()
rebalanceKeys opt@Options{..} st@MutState{..} oldActiveServers = do
  putStrLn $ cyan "Starting rebalancing keys"
  ep <- atomically $ readTVar epoch
  newActiveServers <- atomically $ readTVar activeServers
  let removedServers = oldActiveServers \\ newActiveServers
  let addedServers   = newActiveServers \\ oldActiveServers
  putStrLn $ cyan $ "Removed servers: " ++ show removedServers
  putStrLn $ cyan $ "Added servers: " ++ show addedServers
  kvs <- atomically $ ListT.toList $ M.stream keyStore
  -- When a server is removed and we have to copy some keys to another server
  let toCopy = mapMaybe (\kv@(k, v) ->
        let oldBuckets = bucketAllocator k oldActiveServers fst in
        let newBuckets = bucketAllocator k newActiveServers fst in
        if null (oldBuckets `intersect` removedServers) then Nothing
        else Just (kv, newBuckets \\ oldBuckets)
        ) kvs
  toCopyBuckets <- atomically $ collectUnderBuckets toCopy
  putStrLn $ cyan $ "To copy buckets: " ++ show toCopyBuckets
  forM_ toCopyBuckets $ \((uuidStr, addrStr), kvs) -> do
    let (host, port) = addrStringPair addrStr
    getResponse opt st (Rebalance kvs ep) host [port] >>= \case
      Left err ->
        putStrLn $ red $ "Rebalancing request failed for " ++ addrStr
      Right (Executed _ Ok) ->
        putStrLn $ green $ "Rebalancing complete for " ++ addrStr
      Right _ ->
        putStrLn $ red $ "Incorrect rebalancing response from " ++ addrStr

  -- When a server is added and we have to move some keys to another server
  let unnecessaryKeys = mapMaybe (\kv@(k, v) ->
        let oldBuckets = bucketAllocator k oldActiveServers fst in
        let newBuckets = bucketAllocator k newActiveServers fst in
        if null (newBuckets `intersect` addedServers) then Nothing
        else Just (kv, newBuckets \\ oldBuckets)
        ) kvs
  unnecessaryKeysBuckets <- atomically $ collectUnderBuckets unnecessaryKeys
  putStrLn $ cyan $ "Unnecessary keys buckets: " ++ show unnecessaryKeysBuckets
  forM_ unnecessaryKeysBuckets $ \((uuidStr, addrStr), kvs) -> do
    let (host, port) = addrStringPair addrStr
    getResponse opt st (Rebalance kvs ep) host [port] >>= \case
      Left err ->
        putStrLn $ red $ "Rebalancing request failed for " ++ addrStr
      Right (Executed _ Ok) -> do
        -- The keys are stored in a new server now, their copies in
        -- this servers should be deleted
        atomically $ forM_ kvs $ \(k, _) -> M.delete k keyStore
        putStrLn $ green $ "Rebalancing complete for " ++ addrStr
      Right _ ->
        putStrLn $ red $ "Incorrect rebalancing response from " ++ addrStr

getResponse :: Options
            -> MutState
            -> ServerCommand -- ^ The command to send. Changes the default values for host and port.
            -> HostName -- ^ The host to try to connect.
            -> [ServiceName] -- ^ The ports to try to connect.
            -> IO (Either RequestError Response)
getResponse opt@Options{..} st@MutState{..} c host ports = do
    i <- atomically $ readTVar nextRequestId
    atomically $ modifyTVar' nextRequestId (+1)
    attempt <- findAndConnectOpenPort host ports
    case attempt of
      Nothing -> return $ Left $ CouldNotConnect ports
      Just (sock, sockAddr) -> do
        either <- timeoutAct
          (sendWithLen sock $ S.encode (i, c))
          (return $ Left SendingTimeout) $ \() ->
            timeoutAct (recvWithLen sock) (return $ Left ReceivingTimeout) $ \r ->
              case JSON.decode (BL.fromStrict r) :: Maybe Response of
                Just res -> return $ Right res
                _ -> return $ Left InvalidResponse
        close sock
        return either

-- | The main loop that keeps accepting more connections.
loop :: Socket -> Options -> MutState -> IO ()
loop sock opt st = do
  conn <- accept sock
  forkIO (runConn conn opt st)
  loop sock opt st

run :: Options -> IO ()
run optUser = do
    uuidStr <- newUUID
    let opt = optUser {uuid = uuidStr}
    attempt <- findAndListenOpenPort $ map show [38000..38010]
    case attempt of
      Nothing -> die $ bgRed "Couldn't bind ports 38000 to 38010"
      Just (sock, sockAddr) -> do
        st <- initialState
        let addrStr = show sockAddr
        _ <- forkIO $ sendHeartbeat opt st addrStr -- the initial heartbeat
        setInterval (sendHeartbeat opt st addrStr >> pure True) 5000000 -- every 5 sec
        loop sock opt st
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
  <*> pure "" -- This must be replaced in the run function later

main :: IO ()
main = A.execParser opts >>= run
  where
    opts = A.info (A.helper <*> optionsParser)
      ( A.fullDesc
     <> A.progDesc "Start the server"
     <> A.header "client for an RPC implementation with locks and a view leader" )
