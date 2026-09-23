{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

import Control.Distributed.Process (NodeId (NodeId))
import Control.Distributed.Process.Node (initRemoteTable, newLocalNode, runProcess)
import Control.Distributed.Process.Raft.Client (request, withRaftClient)
import Control.Monad.IO.Class (liftIO)
import qualified Data.Text as Text
import qualified Data.Text.IO as Text.IO
import FileSystem (FileSystemCommand (..), FileSystemResult, mkPath)
import Network.Socket (HostName)
import Network.Transport.TCP (createTransport, defaultTCPAddr, defaultTCPParameters)
import Network.Transport.TCP.Internal (encodeEndPointAddress)
import Options.Applicative

data RuntimeCommand
  = RuntimeCommand
  { rcServer :: HostName,
    rcCommand :: FileSystemCommand
  }

parseRuntimeCommand :: ParserInfo RuntimeCommand
parseRuntimeCommand =
  info
    (runtimeCommand <**> helper)
    (fullDesc <> progDesc "distfs client" <> header "distfs -- client to a distributed in-memory filesystem backed by Raft")
  where
    runtimeCommand =
      RuntimeCommand
        <$> strOption
          ( long "port"
              <> metavar "PORT"
              <> help "Port on which this server listens"
              <> value "4000"
              <> showDefault
          )
        <*> hsubparser
          ( command "touch" (info (TouchFile <$> pathParser) (progDesc "Create a file. If one already exists, this command does nothing."))
              <> command "mkdir" (info (MkDir <$> pathParser) (progDesc "Create a directory."))
              <> command "rm" (info (DeleteFile <$> pathParser) (progDesc "Delete an existing file."))
              <> command "rmdir" (info (DeleteDir <$> pathParser) (progDesc "Delete an existing directory."))
          )

    pathParser = mkPath <$> argument str (metavar "PATH")

main :: IO ()
main = do
  opts <- execParser parseRuntimeCommand
  Right t <- createTransport (defaultTCPAddr "localhost" "0") defaultTCPParameters
  node <- newLocalNode t initRemoteTable

  let serverNodeId = NodeId (encodeEndPointAddress "127.0.0.1" (rcServer opts) 0)

  runProcess node $
    withRaftClient $ \runAction -> do
      runAction (request 1_000_000 serverNodeId (rcCommand opts)) >>= \case
        Left errmsg -> liftIO $ Text.IO.putStrLn $ Text.show errmsg
        Right (_, result :: FileSystemResult) -> liftIO $ print result
