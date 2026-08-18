{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TupleSections #-}

module Main (main) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async
import Control.Concurrent.STM (TQueue, atomically, newTQueueIO, readTQueue, writeTQueue)
import Control.DeepSeq (NFData)
import Control.Monad (replicateM, (>=>))
import Data.Functor ((<&>))
import Data.IntMap.Strict (IntMap)
import qualified Data.IntMap.Strict as IntMap
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.Set (Set)
import qualified Data.Set as Set
import Data.Text (Text)
import Data.Word (Word64)
import GHC.Generics (Generic)
import Network.Consensus.Raft (Config (..), Microseconds, RPC, RPCResult, RaftSpec (..), runRaftT)
import qualified Network.Consensus.Raft as Raft
import Network.Consensus.Raft.Admin (AdminRequest, AdminResponse)
import Network.Consensus.Raft.Client
  ( RaftClientSpec (..),
    Request,
    Response,
    request,
    runRaftClientT,
  )
import Test.Tasty (withResource)
import Test.Tasty.Bench (bench, bgroup, defaultMain, nfIO)

main :: IO ()
main = do
  defaultMain
    [ withResource startCluster stopCluster $ \getClusterHandle ->
        bgroup
          "Benchmark"
          [ unloadedLatency getClusterHandle,
            bgroup
              "Unloaded throughput"
              [ unloadedThroughput getClusterHandle 1,
                unloadedThroughput getClusterHandle 10,
                unloadedThroughput getClusterHandle 100
              ]
          ]
    ]
  where
    unloadedLatency getClusterHandle =
      bench "Unloaded latency " $ nfIO $ do
        cluster <- getClusterHandle
        cluster.runRequest (Get 'a')

    unloadedThroughput getClusterHandle batchSize =
      bench (show batchSize <> " commands") $
        nfIO $
          do
            cluster <- getClusterHandle
            replicateM batchSize (cluster.runRequest (Get 'a'))

data Cluster = Cluster
  { runRequest :: Command -> IO Result,
    runShutDown :: IO ()
  }

startCluster :: IO Cluster
startCluster = do
  harness <- benchHarness clusterScenario
  tid <- async (runServers harness)
  leader <- waitUntilLeaderElected harness
  pure $
    Cluster
      { runRequest = runClientRequest harness leader >=> either (fail . show) (pure . snd),
        runShutDown = cancel tid
      }
  where
    waitUntilLeaderElected harness =
      runClientRequest harness 0 (Get 'a') >>= \case
        Left _ -> threadDelay 10_000 >> waitUntilLeaderElected harness
        Right (leader, _) -> pure leader

    runServers :: Harness -> IO ()
    runServers harness =
      forConcurrently_ (IntMap.elems harness.clusterServers) $ \server ->
        runRaftT
          server.sConfig
          (Raft.InCluster $ Set.fromList (map fromIntegral $ IntMap.keys harness.clusterServers))
          mempty
          server.sSpec
          Raft.server

stopCluster :: Cluster -> IO ()
stopCluster = runShutDown

clusterScenario :: ScenarioInputs
clusterScenario =
  ScenarioInputs
    { heartbeatTimeout = 10_000,
      electionTimeoutLowerBound = 100_000,
      electionTimeoutUpperBound = 300_000,
      seeds = [1 .. 5],
      numInitialClusterNodes = 5
    }

data ScenarioInputs
  = ScenarioInputs
  { heartbeatTimeout :: Microseconds,
    electionTimeoutLowerBound :: Microseconds,
    electionTimeoutUpperBound :: Microseconds,
    seeds :: [Word64],
    numInitialClusterNodes :: Int
  }
  deriving (Eq, Show)

data Server
  = MkServer
  { sConfig :: Config Node,
    sSpec :: RaftSpec Command Node State Result IO
  }

type Mailbox a = IntMap (TQueue a)

data NetworkFabric
  = MkNetworkFabric
  { rpcMailbox :: Mailbox (RPC Node Command State),
    rpcResultsMailbox :: Mailbox (RPCResult Node Result),
    requestsMailbox :: Mailbox (Request Node Command),
    responsesMailbox :: Mailbox (Response Node Result),
    adminMailbox :: Mailbox (AdminRequest Node),
    adminResponsesMailbox :: Mailbox (AdminResponse Node)
  }

newNetworkFabric :: Set Node -> IO NetworkFabric
newNetworkFabric nodes =
  MkNetworkFabric
    <$> newMailbox
    <*> newMailbox
    <*> newMailbox
    <*> newMailbox
    <*> newMailbox
    <*> newMailbox
  where
    newMailbox =
      IntMap.fromList
        <$> traverse
          (\n -> (fromIntegral n,) <$> newTQueueIO)
          (Set.toList nodes)

mkServer :: NetworkFabric -> Microseconds -> Microseconds -> Microseconds -> Word64 -> Node -> Server
mkServer network hbto etolb etoub seed node =
  MkServer
    { sConfig =
        MkConfig
          { nodeId = node,
            electionTimeoutRange = (etolb, etoub),
            heartBeatTimeout = hbto,
            randomSeed = seed,
            maxLogLength = Nothing
          },
      sSpec =
        MkRaftSpec
          { _readLogEntry = \_ _ -> pure Nothing,
            _writeLogEntry = \_ _ _ _ -> pure (),
            _readTerm = \_ -> pure 0,
            _writeTerm = \_ _ -> pure (),
            _readVotedFor = \_ -> pure Nothing,
            _voteFor = \_ _ -> pure (),
            _readSnapshot = \_ -> pure Nothing,
            _writeSnapshot = \_ _ -> pure (),
            _applyLogEntry = step,
            _sendRPC = send network.rpcMailbox,
            _sendRPCResult = send network.rpcResultsMailbox,
            _sendClientResponse = send network.responsesMailbox,
            _sendAdminResponse = send network.adminResponsesMailbox,
            _receiveRPC = receive network.rpcMailbox node <&> Right,
            _receiveRPCResult = receive network.rpcResultsMailbox node <&> Right,
            _receiveClientRequest = receive network.requestsMailbox node <&> Right,
            _receiveAdminRequest = receive network.adminMailbox node <&> Right,
            _tracer = \_ -> pure ()
          }
    }

data Harness
  = MkHarness
  { clusterServers :: IntMap Server,
    runClientRequest :: Node -> Command -> IO (Either Text (Node, Result))
  }

benchHarness ::
  ScenarioInputs ->
  IO Harness
benchHarness
  (ScenarioInputs hb etlb etup s numClusterNodes) = do
    networkFabric <-
      newNetworkFabric $
        mconcat
          [ Set.fromList (fromIntegral <$> serverNodes),
            Set.singleton clientNode,
            Set.singleton adminNode
          ]

    let mkServer' :: Word64 -> Node -> Server
        mkServer' = mkServer networkFabric hb etlb etup

    pure $
      MkHarness
        { clusterServers =
            IntMap.fromList $
              map (\(serverSeed, n) -> (n, mkServer' serverSeed (fromIntegral n))) serverNodesWithSeeds,
          runClientRequest = \leader comm ->
            runRaftClientT
              (request leader comm)
              clientNode
              (clientSpec networkFabric)
        }
    where
      adminNode = -1
      clientNode = Node $ numClusterNodes + 1

      serverNodes = [0 .. numClusterNodes - 1]
      serverNodesWithSeeds = zip (take numClusterNodes s) serverNodes

      clientSpec network =
        MkRaftClientSpec
          { sendRequest = send network.requestsMailbox,
            receiveResponse =
              receive network.responsesMailbox clientNode <&> Right
          }

send :: IntMap (TQueue a) -> Node -> a -> IO ()
send mailbox node message =
  case IntMap.lookup (fromIntegral node) mailbox of
    Nothing -> error $ "Mailbox badly configures: missing node " <> show node
    Just queue -> atomically $ writeTQueue queue message

receive :: IntMap (TQueue a) -> Node -> IO a
receive mailbox node =
  case IntMap.lookup (fromIntegral node) mailbox of
    Nothing -> error $ "Mailbox badly configures: missing node " <> show node
    Just queue -> atomically $ readTQueue queue

-- Simple key-value store

newtype Node = Node Int
  deriving (Eq, Show, Bounded, Enum, Num, Ord, Real, Integral)

type State = Map Char Int

data Command
  = Insert Char Int
  | Delete Char
  | Get Char
  deriving (Eq, Ord, Show)

data Result
  = Value Int
  | Ok
  | Err
  deriving (Eq, Show, Generic)

instance NFData Result

step :: State -> Command -> (State, Result)
step state (Insert k v) = (Map.insert k v state, Ok)
step state (Delete k) = (Map.delete k state, Ok)
step state (Get k) = case Map.lookup k state of
  Nothing -> (state, Err)
  Just v -> (state, Value v)
