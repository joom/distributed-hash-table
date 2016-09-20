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

data Options = Options
  { server :: HostName
  , cmd    :: Command
  } deriving (Show)

-- | Starts a client socket with the given host and port.
getSocket :: HostName -> ServiceName -> IO (Socket, SockAddr)
getSocket host serv = do
    addrInfos <- getAddrInfo Nothing (Just host) (Just serv)
    let serverAddr = head addrInfos
    sock <- socket (addrFamily serverAddr) Stream defaultProtocol
    return (sock, addrAddress serverAddr)

-- | Tries to connect to a port from 38000 to 38010.
-- Returns Nothing if they all fail. Has side effect of printing log messages.
findAndConnectOpenPort :: HostName -> IO (Maybe (Socket, SockAddr))
findAndConnectOpenPort host = foldM (\success port ->
    case success of
      Just _ -> return success -- we already have a successful conn, don't try
      _ -> do
        (sock, sockAddr) <- getSocket host (show port)
        attempt <- timeout 10000000 (try (connect sock sockAddr))
        case (attempt :: Maybe (Either IOException ())) of
          Nothing -> do
            close sock
            putStrLn $ red $
              "Timeout error with socket to " ++ host ++ ":" ++ show port
            return Nothing
          Just (Left e) -> do
            close sock
            putStrLn $ red $ "Couldn't connect to "
                          ++ host ++ ":" ++ show port ++ " because " ++ show e
            return Nothing
          Just (Right ()) -> do
            putStrLn $ green $ "Connected to " ++ host ++ ":" ++ show port
            return $ Just (sock, sockAddr)
  ) Nothing [38000..38010]

-- | Runs the program once it receives a successful parse of the input given.
run :: Options -> IO ()
run Options{..} = do
  attempt <- findAndConnectOpenPort server
  case attempt of
    Nothing ->
      die $ bgRed $ "Couldn't connect to ports 38000 to 38010 on " ++ server
    Just (sock, sockAddr) -> do
      timeoutDie
        (sendWithLen sock $ S.encode (1 :: Int, cmd)) -- 1 is the request ID
        (red "Timeout error when sending")
      r <- timeoutDie (recvWithLen sock) (red "Timeout error when receiving")
      close sock
      B.putStrLn r
      exitSuccess

-- | Parser for a Command, i.e. the procedures available in our RPC system.
commandParser :: Parser Command
commandParser = A.subparser $
     A.command "print"
      (A.info
        (Print <$> A.strArgument (A.metavar "STR" <> A.help "String to print"))
        (A.progDesc "Print a string in the server log."))
  <> A.command "get"
      (A.info
        (Get <$> A.strArgument (A.metavar "KEY" <> A.help "Key to get"))
        (A.progDesc "Get a value from the key store."))
  <> A.command "set"
      (A.info
        (Set <$> A.strArgument (A.metavar "KEY" <> A.help "Key to set")
             <*> A.strArgument (A.metavar "VAL" <> A.help "Value to set"))
        (A.progDesc "Set a value in the key store."))
  <> A.command "query_all_keys"
      (A.info
        (pure QueryAllKeys)
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
