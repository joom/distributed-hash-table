{-# LANGUAGE RecordWildCards, TupleSections, LambdaCase #-}
module ViewLeader where

import Control.Arrow (second)
import Control.Concurrent
import Control.Monad
import Control.Monad.STM
import Control.Concurrent.STM.TVar
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Writer.Strict
import qualified STMContainers.Map as M
import qualified STMContainers.Set as Set
import qualified ListT
import Control.Exception
import qualified Data.Graph as G
import Data.UnixTime
import Data.List (intercalate)
import Data.Maybe
import Data.Hashable
import qualified Data.Sequence.Queue as Q
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
  , addrString        :: AddrString
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
    -- ^ The number of times the view is changed, i.e. a new server was added
    -- or an existing one failed.
  , epoch :: TVar Int
  -- ^ A lock will hold the lock name and an identifier that server calls
  -- itself. This is different from the UUIDString values we hold, they are
  -- arbitrary. If a key exists in the map, then there's a lock on that lock
  -- name. To delete a lock, you have to delete the key from the map.
  -- 'lockMap' maps a lock name to the client id that has it, and to the queue
  -- of client ids that wait for it. The first element of the queue is the
  -- client id that is currently holding the lock. The queue has to hold
  -- unique names. This will later be used to construct a graph to detect a
  -- deadlock.
  , lockMap :: M.Map LockName (Q.Queue ClientId)
  }

initialState :: IO MutState
initialState = MutState <$> M.newIO <*> newTVarIO 0 <*> M.newIO

-- | Runs the command with side effects and returns the response that is to be
-- sent back to the client.
runCommand :: (Int, ViewLeaderCommand) -- ^ A pair of the request ID and a command.
           -> MutState -- ^ A reference to the mutable state.
           -> SockAddr -- ^ Socket address of the client making the request.
           -> IO Response
runCommand (i, cmd) st@MutState{..} sockAddr =
  case cmd of
    Heartbeat uuid addrStr -> do
      now <- getUnixTime
      returnAndLog $ atomically $ runWriterT $ do
        epoch' <- lift $ readTVar epoch
        lift (M.lookup uuid heartbeats) >>= \case
          Just cond@ServerCondition{..} ->
            if isAlive now cond
              then do -- Normal heartbeat update
                lift $ M.insert (cond {lastHeartbeatTime = now}) uuid heartbeats
                logger $ green $ "Heartbeat received from " ++ addrStr
                return $ HeartbeatResponse i Ok epoch'
              else do -- Expired server connection
                logger $ red $ "Expired heartbeat received from " ++ addrStr
                cancelLocksAfterCrash now st
                return $ HeartbeatResponse i Forbidden epoch'
          Nothing -> do -- New server connection
            lift $ M.insert (ServerCondition now addrStr True) uuid heartbeats
            lift $ modifyTVar' epoch (+1)
            logger $ green $ "New heartbeat received from " ++ addrStr
            return $ HeartbeatResponse i Ok epoch'
    QueryServers -> do
      now <- getUnixTime
      returnAndLog $ atomically $ runWriterT $ do
        pairs <- lift $ map (second addrString) . filter (isAlive now . snd) <$> ListT.toList (M.stream heartbeats)
        epoch' <- lift $ readTVar epoch
        logger $ green "Active servers request"
        return $ QueryServersResponse i epoch' pairs
    LockGet name cli ->
      returnAndLog $ atomically $ runWriterT $ do
        lift $ pushToQueueMap st name cli
        detectDeadlock st
        lift (checkLockMap st name) >>= \case
          Nothing -> error "This is impossible"
          Just (x, xs) ->
            if cli == x
            then do
              logger $ green $ "Get lock for \"" ++ name ++ "\" from \"" ++ cli ++ "\""
              return $ Executed i Granted
            else do
              logger $ red $ "Get lock failed for \"" ++ name ++ "\" from \"" ++ cli ++ "\""
              return $ Executed i Retry
    LockRelease name cli ->
      returnAndLog $ atomically $ runWriterT $
        lift (checkLockMap st name) >>= \case
          Just (x, xs) ->
            if cli == x
            then do
              logger $ green $ "Release lock for \"" ++ name ++ "\" from \"" ++ cli ++ "\""
              lift $ popFromQueueMap st name
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

-- Lock abstractions {{{

-- ^ Returns a list of lock names and the client ids of the clients that hold them.
lockHolders :: MutState -> STM [(LockName, ClientId)]
lockHolders MutState{..} =
    mapMaybe (\(n,q) -> (n,) <$> queueHead q) <$> ListT.toList (M.stream lockMap)

-- ^ Checks if the given lock name is held by anyone. If it is, then it returns
-- the entire queue for that lock.
checkLockMap :: MutState -> LockName -> STM (Maybe (ClientId, Q.Queue ClientId))
checkLockMap MutState{..} k = M.lookup k lockMap >>= \case
  Nothing -> return Nothing
  Just q -> (case Q.viewl q of
    Q.EmptyL -> return Nothing
    x Q.:< xs -> return $ Just (x, xs))

-- ^ Pushes the given value to the queue associated with the given key in the
-- given map. If the value is already in the queue, the it is not added.
pushToQueueMap :: MutState -> LockName -> ClientId -> STM ()
pushToQueueMap MutState{..} k v = M.lookup k lockMap >>= \look ->
  M.insert (case look of
    Nothing -> Q.singleton v
    Just q -> if v `qElem` q then q else q Q.|> v) k lockMap

-- ^ Removes the first element of the queue and updates the queue map
-- with the rest of the queue, returns the second
popFromQueueMap :: MutState -> LockName -> STM (Maybe ClientId)
popFromQueueMap MutState{..} k =
  M.lookup k lockMap >>= \case
    Nothing -> return Nothing
    Just q -> case Q.viewl q of
      Q.EmptyL -> return Nothing
      x Q.:< xs -> do
        case Q.viewl xs of -- if the rest is empty, delete the key from map
          Q.EmptyL -> M.delete k lockMap
          _ -> M.insert xs k lockMap
        return $ Just x

-- | Goes through all the servers that expired, but not yet marked inactive.
-- For each of them, it looks for locks held by those servers, releases them.
-- Then marks that server inactive.
cancelLocksAfterCrash :: UnixTime -- ^ Function call time, i.e. now
                      -> MutState
                      -> Logger STM ()
cancelLocksAfterCrash now st@MutState{..} = do
    hbList <- lift $ filter (\(_, c) -> isActive c && not (isAlive now c))
                   <$> ListT.toList (M.stream heartbeats)
    lockList <- lift $ lockHolders st
    forM_ hbList (\(uuid, cond@ServerCondition{..}) -> do
      let portName = dropWhile (/= ':') addrString -- will get something like ":38000"
      let lockNamesToDelete = map snd $ filter (\(reqId, _) -> reqId == portName) lockList
      unless (null lockNamesToDelete) $ do -- Remove locks
        logger $ red $ "Removing locks taken by inactive servers: "
                    ++ intercalate ", " lockNamesToDelete
        lift $ forM_ lockNamesToDelete $ \name ->
          M.delete name lockMap
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
waitedLocks MutState{..} =
    map (second (removeFirst . queueToList)) <$> ListT.toList (M.stream lockMap)
  where
    removeFirst [] = []
    removeFirst (x:xs) = xs

data GraphContent = Lock | Requester

-- | Detects a deadlock and logs the locks and requesters involved.
detectDeadlock :: MutState -> Logger STM ()
detectDeadlock st@MutState{..} = do
  lockList <- lift $ lockHolders st
  lockWaitList <- lift $ waitedLocks st
  let (g, vertexFn) = G.graphFromEdges' $
        -- the lock 'name' is held by the client 'cli'
        map (\(name, cli) -> (Lock, cli, [name])) lockList
        -- the clients 'clis' are waiting for the lock name `name`
        ++ map (\(name, clis) -> (Requester, name, clis)) lockWaitList
  let deadlocks = map (map vertexFn) (cycles g)
  unless (null deadlocks) $
    forM_ deadlocks $ \deadlock -> do
      logger $ bgRed "Deadlock!"
      forM_ deadlock $ \(content, key, values) ->
        case content of
          Requester ->
            logger $ red $ "The lock \"" ++ key ++ "\" is held by " ++ intercalate ", " values
          Lock -> do
            -- TODO something to fix the deadlock
            logger $ red $ "The client \"" ++ key ++ "\" is waiting for " ++ intercalate ", " values

-- }}} End of lock abstractions

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
