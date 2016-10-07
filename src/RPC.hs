{-# LANGUAGE DeriveGeneric, OverloadedStrings, LambdaCase #-}
module RPC where

import Data.Aeson
import Data.Serialize
import GHC.Generics
import Network.Socket hiding (recv)
import Network.Socket.ByteString (recv, sendAll)
import qualified Data.ByteString.Char8 as B
import System.Timeout
import System.Exit

-- Data types and typeclass instances

data Command =
    Print String
  | Get String
  | Set String String
  | QueryAllKeys
  deriving (Show, Eq, Generic)

instance Serialize Command

data Status =
    Ok
  | NotFound
  deriving (Show, Eq)

instance ToJSON Status where
  toJSON Ok = String "ok"
  toJSON NotFound = String "not_found"

data Response =
    Executed     { i :: Int , status :: Status }
  | GetResponse  { i :: Int , status :: Status , value :: String }
  | KeysResponse { i :: Int , status :: Status , keys :: [String] }
  deriving (Show, Eq, Generic)

instance ToJSON Response

-- Message length values and functions

-- | Standardize the size of the messages in which we send the length of the
-- actual message we will send later.
msgLenBytes :: Int
msgLenBytes = 8

-- | Convert an Int to a String for a given number of max bytes. The max number
-- of bytes should be greater than or equal to the number of digits in the
-- initial Int.
intWithCompleteBytes :: Int -- ^ Int that we want to return
                     -> Int -- ^ How many bytes we want to have in the string
                     -> String
intWithCompleteBytes n bytes = let s = show n in
  if length s < bytes then replicate (bytes - length s) '0' ++ s else s

-- Abstractions to deal with sockets and sending the length first.

-- | @'Network.Socket'@ doesn't provide a way to pass the 'MSG_WAITALL' flag to
-- recv, which means that recv (and its variants) may return less than the
-- requested number of bytes. The correct behavior in this case is to
-- repeatedly call 'recv' until the total number of returned bytes is equal to
-- that expected.
safeRecv :: Socket -> Int -> IO B.ByteString
safeRecv sock i = do
  s <- recv sock i
  let len = B.length s
  if len < i
    then B.append s <$> safeRecv sock (i - len)
    else return s

-- | First receives the length of the content it will later receive,
-- then receives the content itself using that length.
recvWithLen :: Socket -> IO B.ByteString
recvWithLen sock = do
    lenStr <- safeRecv sock msgLenBytes
    let lenInt = read (B.unpack lenStr) :: Int
    safeRecv sock lenInt

-- | First sends the length of the content, then sends the content itself.
sendWithLen :: Socket -> B.ByteString -> IO ()
sendWithLen sock msg = do
    let len = B.length msg
    sendAll sock (B.pack (intWithCompleteBytes len msgLenBytes))
    sendAll sock msg

-- Timeout IO

-- | Standardization of timeout limits.
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
