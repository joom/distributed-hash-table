{-# LANGUAGE DeriveGeneric, DefaultSignatures, OverloadedStrings #-}
module RPC.Language where

import Data.Aeson
import Data.Serialize
import GHC.Generics

msgLenBytes :: Int
msgLenBytes = 8

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
    Executed     { status :: Status }
  | GetResponse  { status :: Status , value :: String }
  | KeysResponse { status :: Status , keys :: [String] }
  deriving (Show, Eq, Generic)

instance ToJSON Response

-- | Convert an Int to a String for a given number of max bytes.
intWithCompleteBytes :: Int -- ^ Int that we want to return
                     -> Int -- ^ How many bytes we want to have in the string
                     -> String
intWithCompleteBytes n bytes = let s = show n in
  if length s < bytes then replicate (bytes - length s) '0' ++ s else s
