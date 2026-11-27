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
import Control.Concurrent.Async (Async, async, cancel, forConcurrently_)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import Control.Concurrent.STM (TQueue, atomically, flushTQueue, newTQueueIO, readTQueue, retry, writeTQueue)
import Control.DeepSeq (NFData)
import Control.Monad (forever, unless)
import Data.Functor ((<&>))
import Data.IORef (IORef, newIORef, readIORef, writeIORef)
import Data.IntMap.Strict (IntMap)
import qualified Data.IntMap.Strict as IntMap
import Data.List.NonEmpty (NonEmpty, nonEmpty)
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.Set (Set)
import qualified Data.Set as Set
import Data.Word (Word64)
import Distributed.Consensus.Raft (Config (..), Implementation (..), Microseconds, Networking (..), Persistence (..), RPC, RPCResult, StateMachineStep (..), ephemeralPersistence, runRaftServer)
import qualified Distributed.Consensus.Raft as Raft
import Distributed.Consensus.Raft.Admin (AdminRequest, AdminResponse)
import Distributed.Consensus.Raft.Client
  ( ClientError (..),
    ClientImplementation (..),
    ClientRequest,
    ClientResponse,
    newRequest,
    sendClientRequest,
    sessionId,
    withClientSession,
  )
import GHC.Generics (Generic)
import Test.Tasty (TestTree, localOption, withResource)
import Test.Tasty.Bench (TimeMode (WallTime), bench, bgroup, defaultMain, nfIO)

main :: IO ()
main = do
  defaultMain
    [ localOption WallTime $
        bgroup
          "Benchmark"
          [ benchmarksByTechnology EphemeralStorage,
            benchmarksByTechnology (SimulatedDisk 1_000)
          ]
    ]

totalCommands :: Int
totalCommands = 128

clientCounts :: [Int]
clientCounts = [1, 8, 32, 128]

sustainedCommandsPerClient :: Int
sustainedCommandsPerClient = 20

sustainedClientCounts :: [Int]
sustainedClientCounts = [8, 32, 128, 512]

maxClients :: Int
maxClients = maximum (clientCounts ++ sustainedClientCounts)

data Workload
  = -- | Only reads
    Reads
  | -- | Only mutation
    Writes
  deriving (Show)

runWorkload :: Cluster -> Workload -> Int -> Int -> IO ()
runWorkload cluster w nClients total =
  forConcurrently_ (zip [0 ..] (take nClients cluster.clients)) $ \(c, runCommand) ->
    mapM_ (runCommand . workloadCommand) [c, c + nClients .. total - 1]
  where
    workloadCommand ix =
      case w of
        Reads -> Get key
        Writes -> Insert key ix
      where
        key = toEnum (fromEnum 'a' + ix `mod` 128)

-- TODO: also parametrize networking for "instantaneous" and "simulated latency"
benchmarksByTechnology :: Storage -> TestTree
benchmarksByTechnology storage =
  withResource (startCluster storage) stopCluster $ \getCluster ->
    bgroup
      (show storage)
      [ bgroup
          "Unloaded latency (1 client)"
          [ bench (show workload) $ nfIO $ do
              cluster <- getCluster
              runWorkload cluster workload 1 1
          | workload <- [Reads, Writes]
          ],
        bgroup
          (show totalCommands <> " commands")
          [ bgroup
              (show workload)
              [ bench (show nClients <> " clients") $ nfIO $ do
                  cluster <- getCluster
                  runWorkload cluster workload nClients totalCommands
              | nClients <- clientCounts
              ]
          | workload <- [Reads, Writes]
          ],
        bgroup
          ("Sustained load " <> show sustainedCommandsPerClient <> " commands per client")
          [ bgroup
              (show workload)
              [ bench (show nClients <> " clients") $ nfIO $ do
                  cluster <- getCluster
                  runWorkload cluster workload nClients (nClients * sustainedCommandsPerClient)
              | nClients <- sustainedClientCounts
              ]
          | workload <- [Reads, Writes]
          ]
      ]

data Cluster = Cluster
  { clients :: [Command -> IO Result],
    runShutDown :: IO ()
  }

startCluster :: Storage -> IO Cluster
startCluster storage = do
  harness <- benchHarness storage clusterScenario
  serversTid <- async (runServers harness)

  firstLeaderRef <- newIORef 0
  (firstTid, firstClient) <- startClient harness firstLeaderRef (clientNodeId 0)
  -- Blocking here until the leader is known
  _ <- firstClient (Get 'a')

  leader <- readIORef firstLeaderRef
  others <-
    traverse
      (\i -> newIORef leader >>= \ref -> startClient harness ref (clientNodeId i))
      [1 .. maxClients - 1]

  pure $
    Cluster
      { clients = firstClient : map snd others,
        runShutDown = mapM_ cancel (firstTid : map fst others) >> cancel serversTid
      }
  where
    runServers :: Harness -> IO ()
    runServers harness =
      forConcurrently_ (IntMap.elems harness.clusterServers) $ \server ->
        runRaftServer
          server.sConfig
          (Raft.InCluster $ Set.fromList (map fromIntegral $ IntMap.keys harness.clusterServers))
          mempty
          server.sSpec

    startClient :: Harness -> IORef Node -> Node -> IO (Async (), Command -> IO Result)
    startClient harness leaderRef self = do
      runnerVar <- newEmptyMVar
      tid <- async $
        withClient harness self $ \runClientRequest -> do
          putMVar runnerVar (request runClientRequest)
          forever (threadDelay 1_000_000)

      (tid,) <$> takeMVar runnerVar
      where
        request run command = do
          leader <- readIORef leaderRef
          run leader command >>= \case
            Right (actualLeader, result) -> writeIORef leaderRef actualLeader >> pure result
            Left (Redirect actualLeader) -> writeIORef leaderRef actualLeader >> request run command
            Left _ -> threadDelay 1_000 >> request run command

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
    sSpec :: Implementation Command Node Result State IO
  }

type Mailbox a = IntMap (TQueue a)

data NetworkFabric
  = MkNetworkFabric
  { rpcMailbox :: Mailbox (RPC Command Node State),
    rpcResultsMailbox :: Mailbox (RPCResult Node Result),
    requestsMailbox :: Mailbox (ClientRequest Command Node),
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

mkServer :: Storage -> NetworkFabric -> Microseconds -> Microseconds -> Microseconds -> Word64 -> Node -> Server
mkServer storage network hbto etolb etoub seed node =
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
          { persistence = storagePersistence storage,
            stateMachine = step,
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
    -- the whole benchmark: see 'withClientSession'.
    withClient ::
      forall b.
      Node ->
      ((Node -> Command -> IO (Either (ClientError Command Node) (Node, Result))) -> IO b) ->
      IO b
  }

clientNodeId :: Int -> Node
clientNodeId x = Node $ clusterScenario.numInitialClusterNodes + 1 + x -- +1 for the admin node

benchHarness ::
  Storage ->
  ScenarioInputs ->
  IO Harness
benchHarness
  storage
  (ScenarioInputs hb etlb etup s numClusterNodes) = do
    networkFabric <-
      newNetworkFabric $
        mconcat
          [ Set.fromList (fromIntegral <$> serverNodes),
            Set.fromList (clientNodeId <$> [0 .. maxClients - 1]),
            Set.singleton adminNode
          ]

    let mkServer' :: Word64 -> Node -> Server
        mkServer' = mkServer storage networkFabric hb etlb etup

    pure $
      MkHarness
        { clusterServers =
            IntMap.fromList $
              map (\(serverSeed, n) -> (n, mkServer' serverSeed (fromIntegral n))) serverNodesWithSeeds,
          withClient = \clientNode useClient ->
            withClientSession clientNode (clientSpec networkFabric clientNode) $ \runSession ->
              useClient (\leader comm -> runSession (newRequest comm >>= sendClientRequest maxBound leader))
        }
    where
      adminNode = -1

      serverNodes = [0 .. numClusterNodes - 1]
      serverNodesWithSeeds = zip (take numClusterNodes s) serverNodes

      clientSpec network clientNode =
        ClientImplementation
          { sendRequest = send network.requestsMailbox,
            receiveResponse =
              receive network.responsesMailbox clientNode,
            -- We should generate a random session ID in normal circumstances,
            -- but for a benchmark this doesn't matter
            generateSessionId = pure . sessionId 0 . fromIntegral
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

-- Storage technologies

data Storage
  = EphemeralStorage
  | SimulatedDisk Microseconds
  deriving (Eq)

instance Show Storage where
  show EphemeralStorage = "ephemeral storage"
  show (SimulatedDisk us) =
    "simulated disk (latency = "
      <> show (fromIntegral us `div` (1000 :: Int))
      <> "ms"

storagePersistence :: Storage -> Persistence Command Node State IO
storagePersistence EphemeralStorage = ephemeralPersistence
storagePersistence (SimulatedDisk latency) =
  let durableWrite = threadDelay (fromIntegral latency)
   in ephemeralPersistence
        { writeLogEntry = \_ entries -> unless (null entries) durableWrite,
          truncateLogEntriesFrom = \_ _ -> durableWrite,
          writeTerm = \_ _ -> durableWrite,
          writeVotedFor = \_ _ _ -> durableWrite,
          writeSnapshot = \_ _ -> durableWrite,
          writeCommitIndex = \_ _ -> pure () -- does not need to be durable
        }

-- Simple key-value store

newtype Node = Node Int
  deriving (Eq, Show, Bounded, Enum, Num, Ord, Real, Integral)

type State = Map Char Int

data Command
  = Insert !Char !Int
  | Delete !Char
  | Get !Char
  deriving (Eq, Ord, Show)

data Result
  = Value !Int
  | Ok
  | Err
  deriving (Eq, Show, Generic)

instance NFData Result

step :: Command -> StateMachineStep Result State
step (Insert k v) = Transition $ \state -> (Map.insert k v state, Ok)
step (Delete k) = Transition $ \state -> (Map.delete k state, Ok)
step (Get k) = Read $ \state -> maybe Err Value (Map.lookup k state)
