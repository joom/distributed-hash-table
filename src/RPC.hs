{-# LANGUAGE DeriveGeneric, OverloadedStrings, LambdaCase #-}
module RPC where

import Data.Aeson
import qualified Data.UUID
import Data.Serialize
import Data.Hashable
import GHC.Generics
import Network.Socket hiding (recv)
import Network.Socket.ByteString
import qualified Data.ByteString.Char8 as B
import System.Timeout
import System.Random
import System.Exit
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Writer.Strict
import Control.Concurrent
import Control.Exception
import Control.Monad
import Data.Array
import Data.Graph
import Data.List
import qualified STMContainers.Map as M
import Control.Monad.STM
import qualified Data.Sequence.Queue as Q

-- Data types and typeclass instances

type UUIDString = String
type AddrString = String -- of the format "localhost:8000"
type Epoch = Int
type CommitId = Int

data ServerCommand =
    Get { k :: String }
  | GetR { k :: String , epochInput :: Epoch }
  | SetRVote { k :: String , v :: String , epochInput :: Epoch }
  | SetRCommit { k :: String , commitId :: Int }
  | SetRCancel { k :: String , commitId :: Int }
  | QueryAllKeys
  | Rebalance { kvs :: [(String, String)] , epochInput :: Epoch }
  deriving (Show, Eq, Generic)

instance Serialize ServerCommand

data ViewLeaderCommand =
    Heartbeat UUIDString AddrString -- ^ Takes the server's UUID and port.
  | QueryServers
  | LockGet String UUIDString -- ^ Takes a lock name and a requester UUID.
  | LockRelease String UUIDString -- ^ Takes a lock name and a requester UUID.
  deriving (Show, Eq, Generic)

instance Serialize ViewLeaderCommand

data Status =
    Ok
  | NotFound
  | Retry
  | Granted
  | Forbidden
  deriving (Show, Eq)

instance ToJSON Status where
  toJSON st = String $ case st of
    Ok        -> "ok"
    NotFound  -> "not_found"
    Retry     -> "retry"
    Granted   -> "granted"
    Forbidden -> "forbidden"

instance FromJSON Status where
  parseJSON s = case s of
    String "ok"        -> pure Ok
    String "not_found" -> pure NotFound
    String "retry"     -> pure Retry
    String "granted"   -> pure Granted
    String "forbidden" -> pure Forbidden
    _                  -> mempty

data Response =
    Executed
      { i :: Int , status :: Status }
  | GetResponse
      { i :: Int , status :: Status , value :: String }
  | ExecutedR
      { i :: Int , status :: Status , epoch :: Epoch }
  -- | GetResponseR
  --     { i :: Int , status :: Status , value :: String , epoch :: Epoch }
  | SetResponseR
      { i :: Int , status :: Status , epoch :: Epoch , storedId :: CommitId  }
  | KeysResponse
      { i :: Int , status :: Status , keys :: [String] }
  | QueryServersResponse
      { i :: Int , epoch :: Epoch , result :: [(UUIDString, AddrString)] }
  | HeartbeatResponse
      { i :: Int , status :: Status , epoch :: Epoch }
  deriving (Show, Eq, Generic)

instance ToJSON Response
instance FromJSON Response

newUUID :: IO UUIDString
newUUID = Data.UUID.toString <$> randomIO

-- Message length values and functions

-- | Standardize the size of the messages in which we send the length of the
-- actual message we will send later.
msgLenBytes :: Int
msgLenBytes = 8

-- | Convert an Int to a String for a given number of max bytes. The max number
-- of bytes should be greater than or equal to the number of digits in the
-- initial Int.
-- This is kind of hacky, it should be replaced with Data.Binary.
intWithCompleteBytes :: Int -- ^ Int that we want to return
                     -> Int -- ^ How many bytes we want to have in the string
                     -> String
intWithCompleteBytes n bytes = let s = show n in
  if length s < bytes then replicate (bytes - length s) '0' ++ s else s

-- Timeout and interval IO

-- | Forks a thread to loop an action with given the given waiting period.
setInterval :: IO Bool -- ^ Action to perform. Loop continues if it's True.
            -> Int -- ^ Wait between repetitions in microseconds.
            -> IO ()
setInterval action microsecs = do
    forkIO loop
    return ()
  where
    loop = do
      threadDelay microsecs
      b <- action
      when b loop

-- | Standardization of timeout limits in microseconds.
timeoutTime :: Int
timeoutTime = 10000000 -- 10 seconds

-- | Runs the given action and handles the success with the given function. Has
-- a fallback for what will happen if a timeout occurs. This is basically a
-- tidier abstraction to be used instead of cases.
timeoutAct :: IO a -- ^ Action to be performed in the first place.
           -> IO b -- ^ What to do when there's a timeout error.
           -> (a -> IO b) -- ^ Function to handle success in the action.
           -> IO b
timeoutAct act fail f = do
  m <- timeout timeoutTime act
  case m of
    Nothing -> fail
    Just x -> f x

-- | Runs the given action, kills the application with the given string as the
-- error message if a timeout occurs.
timeoutDie :: IO a -> String -> IO a
timeoutDie act dieStr = timeoutAct act (die dieStr) return

-- Simple logging abstractions

type Logger m a = WriterT [String] m a

logger :: Monad m => String -> Logger m ()
logger s = tell [s]

returnAndLog :: IO (a, [String]) -> IO a
returnAndLog v = do
  (res, logs) <- v
  mapM_ putStrLn logs
  return res

-- | A function that finds all cycles in a graph.  A cycle is given as a
-- finite list of the vertices in order of occurrence, where each vertex
-- only appears once. Written by Chris Smith, April 20, 2009.
-- <https://cdsmith.wordpress.com/2009/04/20/code-for-manipulating-graphs-in-haskell/>
cycles :: Graph -> [[Vertex]]
cycles g = concatMap cycles' (vertices g)
  where cycles' v   = build [] v v
        build p s v =
          let p'         = p ++ [v]
              local      = [ p' | x <- (g!v), x == s ]
              good w     = w > s && not (w `elem` p')
              ws         = filter good (g ! v)
              extensions = concatMap (build p' s) ws
          in  local ++ extensions

-- Simple queue abstractions for locks

type LockName = String
type ClientId = String

queueToList :: Q.Queue a -> [a]
queueToList q = case Q.viewl q of
  Q.EmptyL -> []
  x Q.:< xs -> x : queueToList xs

queueHead :: Q.Queue a -> Maybe a
queueHead q = case Q.viewl q of
  Q.EmptyL -> Nothing
  x Q.:< _ -> Just x

-- ^ Checks if an element is in a queue.
qElem :: Eq a => a -> Q.Queue a -> Bool
qElem e q = case Q.viewl q of
  Q.EmptyL -> False
  x Q.:< xs -> (x == e) || qElem e xs

-- Hashing abstractions

replicationCount :: Int
replicationCount = 3

-- ^ Takes a key and returns which buckets that key should be stored in.  The
-- first one will be the primary one, and the next two will be the backup
-- buckets. If the list of all buckets has at least replicationCount elements,
-- then this function will always return a list of replicationCount elements.
bucketAllocator :: String -- ^ The key we want to add.
                -> [a] -- ^ List of buckets, in some abstraction.
                -> (a -> UUIDString) -- ^ A function to extract the UUIDString from the abstraction.
                -> [a] -- ^ a maximum of replicationCount elements.
bucketAllocator s xs f =
    if length after >= replicationCount
    then take replicationCount after
    else after ++ take (replicationCount - length after) until
  where
    xs' = sortBy (\a b -> compare (hash (f a)) (hash (f b))) xs
    (until, after) = partition ((hash s <) . hash . f) xs'
