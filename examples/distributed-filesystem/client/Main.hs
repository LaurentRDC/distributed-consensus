{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

import Control.Distributed.Process (NodeId (NodeId))
import Control.Distributed.Process.Node (initRemoteTable, newLocalNode, runProcess)
import Control.Distributed.Process.Raft.Client (ClientError (Redirect), request, withRaftClient)
import Control.Monad.IO.Class (liftIO)
import qualified Data.Text as Text
import qualified Data.Text.IO as Text.IO
import FileSystem (FileSystemCommand (..), FileSystemResult (..))
import Network.Socket (HostName)
import Network.Transport.TCP (createTransport, defaultTCPAddr, defaultTCPParameters)
import Network.Transport.TCP.Internal (encodeEndPointAddress)
import Options.Applicative
import System.Exit (exitFailure)

data RuntimeCommand
  = RuntimeCommand
  { rcServer :: HostName,
    rcCommand :: FileSystemCommand
  }

parseRuntimeCommand :: ParserInfo RuntimeCommand
parseRuntimeCommand =
  info
    (runtimeCommand <**> helper)
    ( fullDesc
        <> header "distfs -- client to a distributed in-memory filesystem backed by Raft"
        <> progDesc "distfs client"
    )
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
          ( command "ls" (info (pure ListDir) (progDesc "List the files in the system."))
              <> command "touch" (info (TouchFile <$> pathParser) (progDesc "Create a file. If one already exists, this command does nothing."))
              <> command "rm" (info (DeleteFile <$> pathParser) (progDesc "Delete an existing file."))
          )

    pathParser = argument str (metavar "PATH")

main :: IO ()
main = do
  opts <- execParser parseRuntimeCommand
  Right t <- createTransport (defaultTCPAddr "localhost" "0") defaultTCPParameters
  node <- newLocalNode t initRemoteTable

  let serverNodeId = NodeId (encodeEndPointAddress "127.0.0.1" (rcServer opts) 0)

  runProcess node $
    withRaftClient $ \runAction ->
      let makeRequest server =
            runAction (request server (rcCommand opts)) >>= \case
              Left (Redirect otherServerNode) -> do
                liftIO $ Text.IO.putStrLn $ "Redirected to " <> Text.show otherServerNode
                makeRequest otherServerNode
              Left errmsg -> liftIO $ do
                Text.IO.putStrLn $ Text.show errmsg
                exitFailure
              Right (_, FileSystemError err) -> liftIO $ do
                Text.IO.putStrLn $ Text.show err
                exitFailure
              Right (_, FilesFound files) -> liftIO $ mapM_ print files
              Right (_, FileTouched file) -> liftIO $ print file
              Right (_, FileDeleted _) -> pure ()
       in makeRequest serverNodeId
