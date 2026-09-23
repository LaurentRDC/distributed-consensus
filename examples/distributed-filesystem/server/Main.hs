{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE UndecidableInstances #-}

import Control.Applicative ((<**>))
import Control.Distributed.Process (NodeId (NodeId), getSelfNode)
import Control.Distributed.Process.Node (initRemoteTable, newLocalNode, runProcess)
import qualified Control.Distributed.Process.Raft as Process (networking)
import Control.Exception (throwIO)
import Control.Monad.IO.Class (liftIO)
import qualified Data.Set as Set
import Distributed.Consensus.Raft (ClusterState (..), Config (..), Implementation (..), runRaftServer)
import FileSystem (Directory (..), FileSystem (..))
import GHC.IO.Handle (BufferMode (LineBuffering), hSetBuffering)
import Network.Socket (HostName)
import Network.Transport.TCP (createTransport, defaultTCPAddr, defaultTCPParameters)
import Network.Transport.TCP.Internal (encodeEndPointAddress)
import Options.Applicative
  ( Alternative (many),
    ParserInfo,
    execParser,
    fullDesc,
    header,
    help,
    helper,
    info,
    long,
    metavar,
    progDesc,
    strOption,
  )
import Server.Persistence (withServerPersistence)
import Server.StateMachine (runFileSystemCommand)
import System.Directory.OsPath (getCurrentDirectory)
import System.IO (stdout)
import System.OsPath (unsafeEncodeUtf, (</>))
import System.Random.Stateful (randomIO)

data RuntimeOpts
  = RuntimeOpts
  { port :: HostName,
    peers :: [HostName]
  }

parseRuntimeOpts :: ParserInfo RuntimeOpts
parseRuntimeOpts =
  info
    (runtimeOpts <**> helper)
    (fullDesc <> progDesc "Start a distfs server" <> header "distfs -- a distributed in-memory filesystem backed by Raft")
  where
    runtimeOpts =
      RuntimeOpts
        <$> strOption
          ( long "port"
              <> metavar "PORT"
              <> help "Port on which this server listens"
          )
        <*> many
          ( strOption
              ( long "peer"
                  <> metavar "PORT"
                  <> help "Port on which a peer listens"
              )
          )

main :: IO ()
main = do
  hSetBuffering stdout LineBuffering

  opts <- execParser parseRuntimeOpts
  t <- either throwIO pure =<< createTransport (defaultTCPAddr "127.0.0.1" (port opts)) defaultTCPParameters
  node <- newLocalNode t initRemoteTable
  seed <- randomIO
  cwd <- getCurrentDirectory

  let clusterState = InCluster (Set.fromList [NodeId (encodeEndPointAddress "127.0.0.1" p 0) | p <- peers opts])

  runProcess node $ do
    network <- Process.networking
    nid <- getSelfNode
    withServerPersistence (cwd </> unsafeEncodeUtf ".distfs" </> unsafeEncodeUtf (port opts)) $ \persist ->
      runRaftServer
        ( MkConfig
            { nodeId = nid,
              electionTimeoutRange = (150_000, 300_000),
              heartBeatTimeout = 50_000,
              randomSeed = seed,
              maxLogLength = Just 100
            }
        )
        clusterState
        (FileSystem {fsRoot = Directory mempty})
        ( Implementation
            { persistence = persist,
              applyLogEntry = runFileSystemCommand,
              networking = network,
              tracer = liftIO . print
            }
        )
