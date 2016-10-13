{-# LANGUAGE DeriveGeneric, OverloadedStrings, LambdaCase #-}
module RPC where

import Data.Aeson
import qualified Data.UUID
import Data.Serialize
import GHC.Generics
import Network.Socket hiding (recv)
import Network.Socket.ByteString (recv, sendAll)
import qualified Data.ByteString.Char8 as B
import System.Timeout
import System.Random
import System.Exit

import Control.Concurrent
import Control.Exception
import Control.Monad

-- Data types and typeclass instances

type UUIDString = String

data ServerCommand =
    Print String
  | Get String
  | Set String String
  | QueryAllKeys
  deriving (Show, Eq, Generic)

instance Serialize ServerCommand

data ViewLeaderCommand =
    Heartbeat UUIDString ServiceName -- ^ Takes the server's UUID and port.
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
    Executed             { i :: Int , status :: Status }
  | GetResponse          { i :: Int , status :: Status , value :: String }
  | KeysResponse         { i :: Int , status :: Status , keys :: [String] }
  | QueryServersResponse { i :: Int , epoch  :: Int    , result :: [String] }
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
