{-# LANGUAGE DeriveGeneric, DefaultSignatures, OverloadedStrings #-}
module RPC where

import Data.Aeson
import Data.Serialize
import GHC.Generics

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
    Executed     { i :: String , status :: Status }
  | GetResponse  { i :: String , status :: Status , value :: String }
  | KeysResponse { i :: String , status :: Status , keys :: [String] }
  deriving (Show, Eq, Generic)

instance ToJSON Response

msgLenBytes :: Int
msgLenBytes = 8

-- | Convert an Int to a String for a given number of max bytes.
intWithCompleteBytes :: Int -- ^ Int that we want to return
                     -> Int -- ^ How many bytes we want to have in the string
                     -> String
intWithCompleteBytes n bytes = let s = show n in
  if length s < bytes then replicate (bytes - length s) '0' ++ s else s
