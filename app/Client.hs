{-# LANGUAGE RecordWildCards #-}

module Client where

import Control.Concurrent (threadDelay)
import Control.Monad
import Control.Exception
import qualified Options.Applicative as A
import Options.Applicative (Parser, (<>))
import Network.Socket hiding (send, sendTo, recv, recvFrom)
import Network.Socket.ByteString
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Lazy.Char8 as BL
import qualified Data.Serialize as S
import qualified Data.Aeson as JSON
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
  { serverAddr     :: HostName
  , viewLeaderAddr :: HostName
  , cmd            :: CommandTypes
  } deriving (Show)

-- | Runs the program once it receives a successful parse of the input given.
run :: Int -- ^ Request number by the program.
    -> Options -- ^ Command line options.
    -> IO ()
run i opt@Options{..} = do
  attempt <- findAndConnectOpenPort addr commandPorts
  case attempt of
    Nothing ->
      die $ bgRed $ "Couldn't connect to ports " ++ head commandPorts
                 ++ " to " ++ last commandPorts ++ " on " ++ addr
    Just (sock, sockAddr) -> do
      timeoutDie
        (sendWithLen sock encoded)
        (red "Timeout error when sending")
      r <- timeoutDie (recvWithLen sock) (red "Timeout error when receiving")
      close sock
      B.putStrLn r
      case JSON.decode (BL.fromStrict r) :: Maybe Response of
        Just (Executed _ Retry) -> do
          putStrLn $ yellow "Waiting for 5 sec"
          threadDelay 5000000 -- wait 5 sec
          run (i + 1) opt
        _ -> exitSuccess
  where
    (commandPorts, addr, encoded) = case cmd of
      ServerCmd c -> (map show [38000..38010], serverAddr, S.encode (i, c))
      ViewCmd c   -> (map show [39000..39010], viewLeaderAddr, S.encode (i, c))

-- | Parser for a ServerCommand, i.e. the procedures available in the system.
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
main = A.execParser opts >>= run 1
  where
    opts = A.info (A.helper <*> optionsParser)
      ( A.fullDesc
     <> A.progDesc "Connect to the server at HOST with the given command"
     <> A.header "client for an RPC implementation with locks and a view leader" )
