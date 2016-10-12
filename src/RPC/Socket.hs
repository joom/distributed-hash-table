module RPC.Socket where

import Network.Socket hiding (recv)
import Network.Socket.ByteString (recv, sendAll)
import qualified Data.ByteString.Char8 as B
import Control.Monad
import Control.Exception
import System.Timeout
import System.Console.Chalk

import RPC

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

-- Server socket

-- | Starts a server socket with the given port.
getServerSocket :: ServiceName -> IO (Socket, SockAddr)
getServerSocket serv = do
    (serverAddr : _) <- getAddrInfo
                        (Just (defaultHints {addrFlags = [AI_PASSIVE]}))
                        Nothing (Just serv)
    sock <- socket (addrFamily serverAddr) Stream defaultProtocol
    return (sock, addrAddress serverAddr)

-- | Tries to listen to a port from the given list.
-- Returns Nothing if they all fail. Has side effect of printing log messages.
findAndListenOpenPort :: [ServiceName] -> IO (Maybe (Socket, SockAddr))
findAndListenOpenPort = foldM (\success port ->
    case success of
      Just _ -> return success -- we already have a successful conn, don't try
      _ -> do
        (sock, sockAddr) <- getServerSocket port
        attempt <- timeout timeoutTime (try (bind sock sockAddr))
        case (attempt :: Maybe (Either IOException ())) of
          Nothing -> do
            close sock
            putStrLn $ red $ "Timeout error when binding port " ++ port
            return Nothing
          Just (Left e) -> do
            close sock
            putStrLn $ red $ "Couldn't bind port "
                          ++ port ++ " because " ++ show e
            return Nothing
          Just (Right ()) -> do
            listen sock 1
            putStrLn $ green $ "Listening to " ++ port
            return $ Just (sock, sockAddr)
  ) Nothing

-- Client sockets

-- | Starts a client socket with the given host and port.
getClientSocket :: HostName -> ServiceName -> IO (Socket, SockAddr)
getClientSocket host serv = do
    (serverAddr : _) <- getAddrInfo Nothing (Just host) (Just serv)
    sock <- socket (addrFamily serverAddr) Stream defaultProtocol
    return (sock, addrAddress serverAddr)

-- | Tries to connect to a port from 38000 to 38010.
-- Returns Nothing if they all fail. Has side effect of printing log messages.
findAndConnectOpenPort :: HostName -> [ServiceName] -> IO (Maybe (Socket, SockAddr))
findAndConnectOpenPort host = foldM (\success port ->
    case success of
      Just _ -> return success -- we already have a successful conn, don't try
      _ -> do
        (sock, sockAddr) <- getClientSocket host port
        attempt <- timeout 10000000 (try (connect sock sockAddr))
        case (attempt :: Maybe (Either IOException ())) of
          Nothing -> do
            close sock
            putStrLn $ red $
              "Timeout error with socket to " ++ host ++ ":" ++ port
            return Nothing
          Just (Left e) -> do
            close sock
            putStrLn $ red $ "Couldn't connect to "
                          ++ host ++ ":" ++ port ++ " because " ++ show e
            return Nothing
          Just (Right ()) -> do
            putStrLn $ green $ "Connected to " ++ host ++ ":" ++ port
            return $ Just (sock, sockAddr)
  ) Nothing

