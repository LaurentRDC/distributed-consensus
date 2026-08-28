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
import Control.Concurrent.Async (async, cancel, forConcurrently_)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import Control.Concurrent.STM (TQueue, atomically, flushTQueue, newTQueueIO, readTQueue, retry, writeTQueue)
import Control.DeepSeq (NFData)
import Control.Monad (forever, replicateM, (>=>))
import Data.Functor ((<&>))
import Data.IntMap.Strict (IntMap)
import qualified Data.IntMap.Strict as IntMap
import Data.List.NonEmpty (NonEmpty, nonEmpty)
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.Set (Set)
import qualified Data.Set as Set
import Data.Text (Text)
import Data.Word (Word64)
import Distributed.Consensus.Raft (Config (..), Implementation (..), Microseconds, Networking (..), Persistence (..), RPC, RPCResult, runRaftServer)
import qualified Distributed.Consensus.Raft as Raft
import Distributed.Consensus.Raft.Admin (AdminRequest, AdminResponse)
import Distributed.Consensus.Raft.Client
  ( ClientImplementation (..),
    ClientRequest,
    ClientResponse,
    request,
    withRaftClientT,
  )
import GHC.Generics (Generic)
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
  serversTid <- async (runServers harness)

  runnerVar <- newEmptyMVar
  sessionTid <-
    async $
      withClient harness $ \runClientRequest -> do
        putMVar runnerVar runClientRequest
        forever (threadDelay 1_000_000)
  runClientRequest <- takeMVar runnerVar

  leader <- waitUntilLeaderElected runClientRequest
  pure $
    Cluster
      { runRequest = runClientRequest leader >=> either (fail . show) (pure . snd),
        runShutDown = cancel sessionTid >> cancel serversTid
      }
  where
    waitUntilLeaderElected runClientRequest =
      runClientRequest 0 (Get 'a') >>= \case
        Left _ -> threadDelay 10_000 >> waitUntilLeaderElected runClientRequest
        Right (leader, _) -> pure leader

    runServers :: Harness -> IO ()
    runServers harness =
      forConcurrently_ (IntMap.elems harness.clusterServers) $ \server ->
        runRaftServer
          server.sConfig
          (Raft.InCluster $ Set.fromList (map fromIntegral $ IntMap.keys harness.clusterServers))
          mempty
          server.sSpec

stopCluster :: Cluster -> IO ()
stopCluster = runShutDown

clusterScenario :: ScenarioInputs
clusterScenario =
  let numNodes = 9
   in ScenarioInputs
        { heartbeatTimeout = 10_000,
          electionTimeoutLowerBound = 100_000,
          electionTimeoutUpperBound = 300_000,
          seeds = [1 .. numNodes],
          numInitialClusterNodes = fromIntegral numNodes
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
    sSpec :: Implementation Command Node State Result IO
  }

type Mailbox a = IntMap (TQueue a)

data NetworkFabric
  = MkNetworkFabric
  { rpcMailbox :: Mailbox (RPC Node Command State),
    rpcResultsMailbox :: Mailbox (RPCResult Node Result),
    requestsMailbox :: Mailbox (ClientRequest Node Command),
    responsesMailbox :: Mailbox (ClientResponse Node Result),
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
        Implementation
          { persistence =
              Persistence
                { readLogEntry = \_ _ -> pure Nothing,
                  writeLogEntry = \_ _ -> pure (),
                  readTerm = \_ -> pure 0,
                  writeTerm = \_ _ -> pure (),
                  readVotedFor = \_ _ -> pure Nothing,
                  writeVotedFor = \_ _ _ -> pure (),
                  readSnapshot = \_ -> pure Nothing,
                  writeSnapshot = \_ _ -> pure ()
                },
            applyLogEntry = step,
            networking =
              Networking
                { sendRPC = send network.rpcMailbox,
                  sendRPCResult = send network.rpcResultsMailbox,
                  sendClientResponse = send network.responsesMailbox,
                  sendAdminResponse = send network.adminResponsesMailbox,
                  receiveRPC = receive network.rpcMailbox node,
                  receiveRPCResult = receive network.rpcResultsMailbox node,
                  receiveClientRequests = receiveAll network.requestsMailbox node,
                  receiveAdminRequest = receive network.adminMailbox node
                },
            tracer = \_ -> pure ()
          }
    }

data Harness
  = MkHarness
  { clusterServers :: IntMap Server,
    -- | Open a client session and run an action with it. One session serves
    -- the whole benchmark: see 'withRaftClientT'.
    withClient ::
      forall b.
      ((Node -> Command -> IO (Either Text (Node, Result))) -> IO b) ->
      IO b
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
          withClient = \useClient ->
            withRaftClientT clientNode (clientSpec networkFabric) $ \runSession ->
              useClient (\leader comm -> runSession (request leader comm))
        }
    where
      adminNode = -1
      clientNode = Node $ numClusterNodes + 1

      serverNodes = [0 .. numClusterNodes - 1]
      serverNodesWithSeeds = zip (take numClusterNodes s) serverNodes

      clientSpec network =
        ClientImplementation
          { sendRequest = send network.requestsMailbox,
            receiveResponse =
              receive network.responsesMailbox clientNode
          }

simulatedNetworkLatency :: IO ()
simulatedNetworkLatency = threadDelay 1_000

send :: IntMap (TQueue a) -> Node -> a -> IO ()
send mailbox node message =
  case IntMap.lookup (fromIntegral node) mailbox of
    Nothing -> error $ "Mailbox badly configures: missing node " <> show node
    Just queue -> simulatedNetworkLatency >> atomically (writeTQueue queue message)

receive :: IntMap (TQueue a) -> Node -> IO a
receive mailbox node =
  case IntMap.lookup (fromIntegral node) mailbox of
    Nothing -> error $ "Mailbox badly configures: missing node " <> show node
    Just queue -> atomically $ readTQueue queue

receiveAll :: IntMap (TQueue a) -> Node -> IO (NonEmpty a)
receiveAll mailbox node =
  case IntMap.lookup (fromIntegral node) mailbox of
    Nothing -> error $ "Mailbox badly configures: missing node " <> show node
    Just queue ->
      atomically $
        flushTQueue queue <&> nonEmpty >>= \case
          Nothing -> retry
          Just xs -> pure xs

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
