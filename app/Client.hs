{-# LANGUAGE RecordWildCards #-}

module Client where

import Control.Monad
import Control.Exception
import qualified Options.Applicative as A
import Options.Applicative (Parser, (<>))
import Network.Socket hiding (send, sendTo, recv, recvFrom)
import Network.Socket.ByteString
import qualified Data.ByteString.Char8 as B
import qualified Data.Serialize as S
import System.Console.Chalk
import System.Timeout
import System.Exit

import RPC
import RPC.Socket

data CommandTypes =
    ServerCmd ServerCommand
  | ViewCmd ViewLeaderCommand
  deriving (Show)

data Options = Options
  { server :: HostName
  , cmd    :: CommandTypes
  } deriving (Show)

-- | Runs the program once it receives a successful parse of the input given.
run :: Options -> IO ()
run Options{..} = do
  let commandPorts = case cmd of {ServerCmd _ -> [38000..38010] ; ViewCmd _ -> [39000..39010]}

  attempt <- findAndConnectOpenPort server $ map show commandPorts
  case attempt of
    Nothing ->
      die $ bgRed $ "Couldn't connect to ports 38000 to 38010 on " ++ server
    Just (sock, sockAddr) -> do
      let i = 1 :: Int -- 1 is the request ID
      let encoded = case cmd of {ServerCmd c -> S.encode (i, c) ; ViewCmd c -> S.encode (i, c)}
      timeoutDie
        (sendWithLen sock encoded)
        (red "Timeout error when sending")
      r <- timeoutDie (recvWithLen sock) (red "Timeout error when receiving")
      close sock
      B.putStrLn r
      exitSuccess

-- | Parser for a ServerCommand, i.e. the procedures available in our RPC system.
commandParser :: Parser CommandTypes
commandParser = A.subparser $
     A.command "print"
      (A.info
        (ServerCmd <$>
          (Print <$> A.strArgument (A.metavar "STR" <> A.help "String to print")))
        (A.progDesc "Print a string in the server log."))
  <> A.command "get"
      (A.info
        (ServerCmd <$>
          (Get <$> A.strArgument (A.metavar "KEY" <> A.help "Key to get")))
        (A.progDesc "Get a value from the key store."))
  <> A.command "set"
      (A.info
        (ServerCmd <$>
          (Set <$> A.strArgument (A.metavar "KEY" <> A.help "Key to set")
               <*> A.strArgument (A.metavar "VAL" <> A.help "Value to set")))
        (A.progDesc "Set a value in the key store."))
  <> A.command "query_all_keys"
      (A.info
        (pure $ ServerCmd QueryAllKeys)
        (A.progDesc "Get all the keys from the key store."))

-- | Parser for the optional parameters of the client.
optionsParser :: Parser Options
optionsParser = Options
  <$> A.strOption
      ( A.long "server"
     <> A.short 's'
     <> A.metavar "SERVER"
     <> A.help "Address of the server to connect"
     <> A.value "localhost" )
  <*> commandParser

main :: IO ()
main = A.execParser opts >>= run
  where
    opts = A.info (A.helper <*> optionsParser)
      ( A.fullDesc
     <> A.progDesc "Connect to the server at HOST with the given command"
     <> A.header "client for a simple RPC implementation" )
