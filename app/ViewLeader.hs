{-# LANGUAGE RecordWildCards, TupleSections#-}
module ViewLeader where

import Control.Concurrent
import Control.Monad
import Control.Monad.STM
import Control.Concurrent.STM.TVar
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Writer.Strict
import Control.Arrow ((&&&))
import qualified STMContainers.Map as M
import qualified STMContainers.Multimap as MM
import qualified STMContainers.Set as Set
import qualified ListT
import Control.Exception
import qualified Data.Graph as G
import Data.UnixTime
import Data.List (intercalate)
import Data.Maybe
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

data ServerCondition = ServerCondition
  { -- ^ Denotes the last valid heartbeat time. Late heartbeats will not update.
    lastHeartbeatTime :: UnixTime
  , serverAddr        :: SockAddr
  , isActive          :: Bool
  }

isAlive :: UnixTime -- ^ Now
         -> ServerCondition
         -> Bool
isAlive now ServerCondition{..} = now < expireTime
  where
    expireTime = lastHeartbeatTime `addUnixDiffTime` secondsToUnixDiffTime 30


-- | A type for mutable state.
data MutState = MutState
  { -- ^ Heartbeats are held in a map. When we don't receive a heartbeat
    -- for 30 sec, then we won't accept any heartbeat with the same UUID.
    heartbeats :: M.Map UUIDString ServerCondition
  , epoch :: TVar Int
  -- ^ A lock will hold the lock name and an identifier that server calls itself.
  -- This is different from the UUIDString values we hold, they are arbitrary.
  -- If a key exists in the map, then there's a lock on that lock name.
  -- To delete a lock, you have to delete the key from the map.
  , locks :: M.Map String String
  -- ^ Maps a client identifier to a set of lock names the client is waiting for.
  -- This will later be used to construct a graph to detect a deadlock.
  , lockWaiting :: MM.Multimap String String
  }

initialState :: IO MutState
initialState = MutState <$> M.newIO <*> newTVarIO 0 <*> M.newIO <*> MM.newIO

-- | Runs the command with side effects and returns the response that is to be
-- sent back to the client.
runCommand :: (Int, ViewLeaderCommand) -- ^ A pair of the request ID and a command.
           -> MutState -- ^ A reference to the mutable state.
           -> SockAddr -- ^ Socket address of the client making the request.
           -> IO Response
runCommand (i, cmd) st@MutState{..} sockAddr =
  case cmd of
    Heartbeat uuid port -> do
      now <- getUnixTime
      returnAndLog $ atomically $ runWriterT $ do
        get <- lift $ M.lookup uuid heartbeats
        case get of
          Just cond@ServerCondition{..} -> do
            if isAlive now cond
              then do -- Normal heartbeat update
                lift $ M.insert (cond {lastHeartbeatTime = now}) uuid heartbeats
                logger $ green $ "Heartbeat received from " ++ port
                return $ Executed i Ok
              else do -- Expired server connection
                logger $ red $ "Expired heartbeat received from " ++ port
                cancelLocksAfterCrash now st
                return $ Executed i Forbidden
          Nothing -> do -- New server connection
            lift $ M.insert (ServerCondition now sockAddr True) uuid heartbeats
            lift $ modifyTVar' epoch (+1)
            logger $ green $ "New heartbeat received from " ++ port
            return $ Executed i Ok
    QueryServers -> do
      now <- getUnixTime
      returnAndLog $ atomically $ runWriterT $ do
        addrs <- lift $ map (show . serverAddr) . filter (isAlive now) . map snd
                        <$> ListT.toList (M.stream heartbeats)
        epoch' <- lift $ readTVar epoch
        logger $ green "Active servers request"
        return $ QueryServersResponse i epoch' addrs
    LockGet name cli ->
      returnAndLog $ atomically $ runWriterT $ do
        get <- lift $ M.lookup name locks
        case get of
          Just uuid -> do
            logger $ red $ "Get lock failed for \"" ++ name ++ "\" from \"" ++ cli ++ "\""
            lift $ MM.insert name cli lockWaiting
            detectDeadlock st
            return $ Executed i Retry
          Nothing -> do
            lift $ M.insert cli name locks
            lift $ MM.delete name cli lockWaiting
            logger $ green $ "Get lock for \"" ++ name ++ "\" from \"" ++ cli ++ "\""
            return $ Executed i Granted
    LockRelease name cli ->
      returnAndLog $ atomically $ runWriterT $ do
        get <- lift $ M.lookup name locks
        case get of
          Just cliInMap ->
            if cliInMap == cli
            then do
              logger $ green $ "Release lock for \"" ++ name ++ "\" from \"" ++ cli ++ "\""
              lift $ M.delete name locks
              return $ Executed i Ok
            else do
              logger $ red $ "Release lock failed for \"" ++ name ++ "\" from \"" ++ cli ++ "\""
              return $ Executed i Forbidden
          Nothing -> do
            logger $ green $ "Release nonexistent lock for \"" ++ name ++ "\" from \"" ++ cli ++ "\""
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
        response <- runCommand (i, cmd) st sockAddr
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

-- | Goes through all the servers that expired, but not yet marked deactive.
-- For each of them, it looks for locks held by those servers, releases them.
-- Then marks that server inactive.
cancelLocksAfterCrash :: UnixTime -- ^ Function call time, i.e. now
                      -> MutState
                      -> Logger STM ()
cancelLocksAfterCrash now MutState{..} = do
    hbList <- lift $ filter (\(_, c) -> isActive c && not (isAlive now c))
                   <$> ListT.toList (M.stream heartbeats)
    lockList <- lift $ ListT.toList (M.stream locks)
    forM_ hbList (\(uuid, cond@ServerCondition{..}) -> do
      let portName = dropWhile (/= ':') (show serverAddr) -- will get something like ":38000"
      let lockNamesToDelete = map fst $ filter (\(_, reqId) -> reqId == portName) lockList
      unless (null lockNamesToDelete) $ do -- Remove locks
        logger $ red $ "Removing locks taken by inactive servers: "
                    ++ intercalate ", " lockNamesToDelete
        lift $ forM_ lockNamesToDelete $ \name -> do
          M.delete name locks
          MM.deleteByKey name lockWaiting
      lift $ do -- Mark inactive
        modifyTVar' epoch (+1)
        M.insert (cond {isActive = False}) uuid heartbeats)

-- | An IO function to call regularly to remove locks held by servers that
-- completely stopped sending heartbeats. If a server sends a heartbeat after
-- it expires, the heartbeat will be seen as an "expired heartbeat", but
-- if a server stops altogether, we have to be able to detect that in time and
-- cancel necessary locks.
cancelLocksAfterCrashIO :: MutState -> IO ()
cancelLocksAfterCrashIO st = do
  now <- getUnixTime
  returnAndLog $ atomically $ runWriterT $ cancelLocksAfterCrash now st

-- | An association list of keys to the requesters waiting for that key.
waitedLocks :: MutState -> STM [(String, [String])]
waitedLocks MutState{..} = do
  keys <- ListT.toList (MM.streamKeys lockWaiting)
  sets <- mapM (\key -> fromJust <$> MM.lookupByKey key lockWaiting) keys
  setsToLists <- mapM (ListT.toList . Set.stream) sets
  return $ zip keys setsToLists

data GraphContent = Lock | Requester

-- | Detects a deadlock and logs the locks and requesters involved.
detectDeadlock :: MutState -> Logger STM ()
detectDeadlock st@MutState{..} = do
  lockList <- lift $ ListT.toList (M.stream locks)
  lockWaitList <- lift $ waitedLocks st
  let (g, vertexFn) = G.graphFromEdges' $
        -- the lock 'name' is held by the client 'cli'
        map (\(name, cli) -> (Lock, name, [cli])) lockList
        -- the client 'cli' is waiting for the lock names in `names`
        ++ map (\(cli, names) -> (Requester, cli, names)) lockWaitList
  let deadlocks = zip [1..] $ map (map vertexFn) (cycles g)
  unless (null deadlocks) $
    forM_ deadlocks $ \(i, deadlock) -> do
      logger $ bgRed $ "Deadlock #" ++ show i
      forM_ deadlock $ \(content, key, values) ->
        case content of
          Lock ->
            logger $ red $ "The lock \"" ++ key ++ "\" is held by " ++ intercalate ", " values
          Requester ->
            logger $ red $ "The client \"" ++ key ++ "\" is waiting for " ++ intercalate ", " values

main :: IO ()
main = do
    attempt <- findAndListenOpenPort $ map show [39000..39010]
    case attempt of
      Nothing -> die $ bgRed "Couldn't bind ports 39000 to 39010"
      Just (sock, sockAddr) -> do
        st <- initialState
        setInterval (cancelLocksAfterCrashIO st >> pure True) 5000000 -- every 5 sec
        loop sock st
        close sock
