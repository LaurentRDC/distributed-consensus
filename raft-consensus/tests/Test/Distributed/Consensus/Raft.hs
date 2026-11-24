{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}

module Test.Distributed.Consensus.Raft
  ( tests,
  )
where

import Control.Applicative ((<|>))
import Control.Concurrent.Class.MonadMVar (MVar, newEmptyMVar, putMVar, takeMVar)
import Control.Concurrent.Class.MonadSTM (MonadSTM, TQueue, TVar, atomically, flushTQueue, newTQueueIO, newTVarIO, readTQueue, readTVar, readTVarIO, retry, writeTQueue, writeTVar)
import qualified Control.Concurrent.Linearizable as Linearizable
import Control.Exception (Exception)
import Control.Monad (replicateM, unless, void, when)
import Control.Monad.Class.MonadAsync (async, asyncThreadId, concurrently_, forConcurrently_, race_, wait, waitCatch, withAsync)
import Control.Monad.Class.MonadFork (ThreadId, throwTo)
import Control.Monad.Class.MonadTest (exploreRaces)
import Control.Monad.Class.MonadTime (getMonotonicTimeNSec)
import Control.Monad.Class.MonadTimer (threadDelay, timeout)
import Control.Monad.IOSim (ExplorationOptions, IOSim, exploreSimTrace, traceM)
import Data.Functor ((<&>))
import Data.IntMap (IntMap)
import qualified Data.IntMap.Strict as IntMap
import qualified Data.IntSet as IntSet
import Data.List (genericLength, zip4)
import Data.List.NonEmpty (NonEmpty, nonEmpty)
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.Maybe (fromJust)
import Data.Set (Set)
import qualified Data.Set as Set
import qualified Data.Text.Lazy as Text
import Data.Word (Word64)
import qualified Debug.Trace as Debug
import Distributed.Consensus.Raft
  ( ClusterConfiguration (..),
    ClusterState,
    Config (..),
    Implementation (..),
    LogEntry,
    LogIndex,
    Networking (..),
    Persistence (..),
    RPC,
    RPCResult,
    RaftTrace (..),
    Snapshot,
    Term,
    runRaftServer,
  )
import qualified Distributed.Consensus.Raft as Raft
import Distributed.Consensus.Raft.Admin
  ( AdminCommandResult (ClusterConfigurationIs),
    AdminError (AdminNotLeader),
    AdminImplementation
      ( AdminImplementation,
        receiveAdminResponse,
        sendAdminRequest
      ),
    AdminRequest,
    AdminResponse,
    Microseconds,
    RaftAdminT,
    sessionId,
    withAdminSession,
  )
import qualified Distributed.Consensus.Raft.Admin as Admin
import Distributed.Consensus.Raft.Client
  ( ClientError (..),
    ClientImplementation (..),
    ClientRequest,
    ClientResponse,
    RaftClientT,
    newRequest,
    sendClientRequest,
    withClientSession,
  )
import qualified Distributed.Consensus.Raft.Client as Client
import System.Random (RandomGen, mkStdGen64, randomR, uniformR)
import Test.Distributed.Consensus.Raft.Client (RaftClientTrace (..), checkLinearizability)
import Test.Distributed.Consensus.Raft.Options (PrintTrace (..), setNumRacyTests, withExplorationOptions, withPrintTraceOption)
import Test.Distributed.Consensus.Raft.Properties (FaultInjection (..), allProperties)
import Test.Distributed.Consensus.Raft.Scenario (checkScenario)
import Test.StateMachine (Command, Node (Node), Result, State, step)
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.QuickCheck
  ( Arbitrary (arbitrary),
    Gen,
    Property,
    Testable (property),
    chooseBoundedIntegral,
    counterexample,
    elements,
    forAll,
    testProperty,
    vectorOf,
    (.&&.),
  )
import Text.Pretty.Simple (pShow)
import Prelude hiding (read)

tests :: TestTree
tests =
  testGroup
    "Raft"
    [ testGroup
        "Property tests"
        [ testGroup
            "No fault injection"
            [ testClusterWithoutRaces NoFaultInjection,
              testClusterWithRaces NoFaultInjection
            ],
          testGroup
            "With fault injection"
            [ testClusterWithoutRaces FaultInjection,
              testClusterWithRaces FaultInjection
            ]
        ]
    ]

testClusterWithoutRaces :: FaultInjection -> TestTree
testClusterWithoutRaces faultInjection =
  withPrintTraceOption $ \printOrNot ->
    testProperty "Cluster properties without schedule exploration" $
      propClusterWith
        printOrNot
        id -- exploration options don't apply to non-racy simulations
        faultInjection
        (pure ())

testClusterWithRaces :: FaultInjection -> TestTree
testClusterWithRaces faultInjection =
  withPrintTraceOption $ \printOrNote ->
    setNumRacyTests $
      withExplorationOptions $ \updateExplorationOptions ->
        testProperty "Cluster properties with schedule exploration" $
          propClusterWith printOrNote updateExplorationOptions faultInjection exploreRaces

propClusterWith ::
  PrintTrace ->
  (ExplorationOptions -> ExplorationOptions) ->
  FaultInjection ->
  (forall s. IOSim s ()) ->
  Property
propClusterWith printTrace updateExplorationOptions faultInjection raceOrNot =
  property $
    forAll (genScenarioInputs faultInjection) $
      \scenarioInputs ->
        counterexample
          (Text.unpack $ pShow scenarioInputs)
          $ exploreSimTrace
            updateExplorationOptions
            (scenario scenarioInputs)
            ( \_ trace ->
                checkScenario
                  (allProperties @Command @Node @Result @State faultInjection)
                  trace
                  .&&. checkLinearizability trace
            )
  where
    debug :: forall a. (Show a) => a -> a
    debug = case printTrace of
      PrintTrace False -> id
      PrintTrace True -> Debug.traceShowId
    scenario scenarioInputs =
      testHarness debug scenarioInputs $ \(harness :: Harness s) -> do
        -- In order to detect infinite loops, especially in CI,
        -- we use a *very generous* scenario timeout.
        let timeStep = max clientRetryTick scenarioInputs.electionTimeoutUpperBound
            scenarioTimeUpperBound =
              10 * timeStep -- Baseline
              -- To ensure all lone nodes have time to join, and then leave
                + 3
                  * maybe 0 maximum (nonEmpty $ sum <$> scenarioInputs.loneNodesWait)
                -- To ensure all client requests have time to be served, we wait
                -- for the client with the most commands as if ever command
                -- was retried for the maximum number of times.
                + maybe 0 maximum (nonEmpty $ genericLength <$> IntMap.elems scenarioInputs.commands)
                  * ( clientRequestTimeout scenarioInputs
                        + clientRetryBudget scenarioInputs
                        + clientRetryTick
                    )
                -- To ensure membership changes have time to land
                + 3
                  * 6 -- 6 attempts
                  * timeStep

        race_
          (threadDelay (fromIntegral scenarioTimeUpperBound) >> fail "Possible infinite loop detected")
          ( do
              withAdminSession harness.hAdminNode harness.hAdminSpec $ \runAdminAction ->
                -- Run servers until the clients are done interacting
                withAsync (runServers harness runAdminAction) $ \_ -> do
                  withAsync (runClients harness scenarioInputs) $ \clients -> do
                    wait clients
                    -- We give an opportunity to all lone servers to leave the cluster
                    -- properly before we shut everyone down.
                    let shutDownNode node =
                          let tryShutDown :: Int -> IOSim s ()
                              tryShutDown 0 = pure ()
                              tryShutDown attemptsLeft =
                                timeout
                                  (fromIntegral scenarioInputs.electionTimeoutUpperBound)
                                  (runAdminAction (Admin.shutDown >>= Admin.sendAdminCommand node <&> void))
                                  >>= maybe
                                    (tryShutDown (attemptsLeft - 1))
                                    (either (const (tryShutDown (attemptsLeft - 1))) pure)
                           in tryShutDown 3

                    forConcurrently_ harness.loneServers $ \(server, isDone, _, _) -> do
                      takeMVar isDone
                      shutDownNode server.sConfig.nodeId
                    forConcurrently_ harness.clusterServers $ \server ->
                      shutDownNode server.sConfig.nodeId
          )

    runServers ::
      Harness s ->
      (forall a. RaftAdminT Node (IOSim s) a -> IOSim s a) ->
      IOSim s ()
    runServers harness runAdminAction = do
      -- This is the point where we can mark this thread "racy"
      -- (by default, it is not).
      --
      -- The benefit of NOT marking this racy is to explore many more of the initial
      -- parameter space (more 'ScenarioInputs's).
      --
      -- The benefit of marking this racy is to explore races within fewer initial
      -- conditions
      raceOrNot
      concurrently_
        -- Cluster nodes
        ( forConcurrently_ (IntMap.elems harness.clusterServers) $ \server ->
            runServerWithFaults
              server
              (Raft.InCluster $ Set.fromList (map fromIntegral $ IntMap.keys harness.clusterServers))
        )
        -- Lone nodes
        ( forConcurrently_ (IntMap.elems harness.loneServers) $ \(server, isDone, waitToJoin, waitToLeave) ->
            concurrently_
              ( do
                  let node = server.sConfig.nodeId
                      -- A node that stayed in the cluster, so we have somebody
                      -- to ask about the committed configuration. Asking the
                      -- node that is leaving would never work: it never learns
                      -- that it is out.
                      contact =
                        fromIntegral $
                          fst $
                            fromJust $
                              IntSet.minView (IntMap.keysSet harness.clusterServers)
                      betweenAttempts = snd server.sConfig.electionTimeoutRange
                      requestTimeout = 3 * server.sConfig.heartBeatTimeout

                  threadDelay (fromIntegral waitToJoin)
                  untilJoined runAdminAction contact requestTimeout betweenAttempts node $
                    Admin.joinCluster contact >>= Admin.sendAdminCommand node

                  threadDelay (fromIntegral waitToLeave)
                  untilLeft runAdminAction contact requestTimeout betweenAttempts node $
                    Admin.leaveCluster >>= Admin.sendAdminCommand node

                  putMVar isDone ()
              )
              (runServerWithFaults server Raft.LoneNode)
        )

    runClients ::
      Harness s ->
      ScenarioInputs ->
      IOSim s ()
    runClients harness scenarioInputs =
      forConcurrently_ (IntMap.toList harness.hClients) $ \(clientNode, clientImplementation) ->
        withClientSession (fromIntegral clientNode) clientImplementation $ \runClientAction ->
          runClient
            (fromIntegral clientNode)
            runClientAction
            -- In principle, there is no upper bound on how long a
            -- client can wait to receive a message. I'm putting a generous
            -- bound here because I have seen situations where the client never
            -- receives a reply (due to a bug)
            (clientRequestTimeout scenarioInputs)
            (clientRetryBudget scenarioInputs)
            (scenarioInputs.commands IntMap.! (clientNode - minClientNode))
      where
        minClientNode = IntSet.findMin $ IntMap.keysSet harness.hClients

        runClient ::
          Node ->
          (forall a. RaftClientT Command Node Result (IOSim s) a -> IOSim s a) ->
          -- \| Single request timeout
          Microseconds ->
          -- \| Timeout for retries
          Microseconds ->
          [Command] ->
          IOSim s ()
        runClient clientNode runRequest maxTime retryBudget = go (Set.singleton (0 :: Node))
          where
            -- \| Given a node, and a set of known nodes, deterministically pick another
            -- node from the set.
            nextCandidate :: Node -> Set Node -> Maybe Node
            nextCandidate failed known = Set.lookupGT failed known <|> Set.lookupMin known

            go _ [] = pure ()
            go knownNodes (command : rest) = do
              req <- runRequest (newRequest command)
              start <- getMonotonicTimeNSec
              attempt req knownNodes retryBudget 0 >>= \case
                Just result -> do
                  end <- getMonotonicTimeNSec
                  traceM (MkRaftClientTrace clientNode (Linearizable.MkEvent start end (command, Just result)))
                Nothing ->
                  traceM (MkRaftClientTrace clientNode (Linearizable.MkEvent start maxBound (command, Nothing @Result)))
              go knownNodes rest
              where
                attempt payload nodeSet remaining target = do
                  threadDelay (fromIntegral clientRetryTick)
                  timeout (fromIntegral maxTime) (runRequest (sendClientRequest target payload))
                    >>= \case
                      Nothing ->
                        giveUp $
                          "no response at all within "
                            <> show (toInteger maxTime)
                            <> " microseconds"
                      -- Command needs to be retried but on another target
                      Just (Left err@(Redirect otherNode))
                        | remaining <= clientRetryTick ->
                            giveUp $
                              "retried for "
                                <> show (toInteger retryBudget)
                                <> " microseconds, last response was "
                                <> show err
                        | otherwise -> attempt payload (Set.insert otherNode nodeSet) (remaining - clientRetryTick) otherNode
                      Just (Left err@NoKnownLeader)
                        | remaining <= clientRetryTick ->
                            giveUp $
                              "retried for "
                                <> show (toInteger retryBudget)
                                <> " microseconds, last response was "
                                <> show err
                        | otherwise -> case nextCandidate target nodeSet of
                            Nothing -> giveUp "No known nodes to query"
                            Just candidate -> attempt payload nodeSet (remaining - clientRetryTick) candidate
                      Just (Right (_, result)) -> pure (Just result)
                  where
                    giveUp why =
                      case faultInjection of
                        -- In a real Raft cluster (i.e. subject to faults), there's no guarantee of liveness.
                        -- That's the price to pay for strong consistency.
                        --
                        -- This means we can be stuck in a situation where a cluster is so faulty, that it
                        -- literally cannot serve any requests -- think of an election + crash loop. Thus,
                        -- giving up in this context means letting go.
                        --
                        -- A Raft cluster that is NOT subject to faults has no excuse for not eventually
                        -- service client requests, hence the use of 'fail'
                        FaultInjection -> pure Nothing
                        NoFaultInjection ->
                          fail $
                            "Client could not get "
                              <> show command
                              <> " served, and no node in this scenario can crash: "
                              <> why

data ScenarioInputs
  = ScenarioInputs
  { heartbeatTimeout :: Microseconds,
    electionTimeoutLowerBound :: Microseconds,
    electionTimeoutUpperBound :: Microseconds,
    seeds :: [Word64],
    faultProbabilities :: [Double],
    partitionSeed :: Word64,
    partitionProbability :: Double,
    numInitialClusterNodes :: Int,
    numInitialLoneNodes :: Int,
    numClientNodes :: Int,
    loneNodesWait :: [(Microseconds, Microseconds)],
    -- | Zero-indexed commands per client node.
    commands :: IntMap [Command]
  }
  deriving (Eq, Show)

genScenarioInputs :: FaultInjection -> Gen ScenarioInputs
genScenarioInputs faultInjection = do
  clusterSize <- elements [1, 2, 3]
  numLoneNodes <- elements [0, 1]
  numClients <- elements [1, 2, 3]
  ss <- vectorOf (clusterSize + numLoneNodes) (chooseBoundedIntegral (0, 1_000_000))
  hb <- chooseBoundedIntegral (clientRetryTick, 200_000)
  -- Making the lower election timeout possibly shorter than the heartbeat timeout
  -- allows to have terms with no leaders elected
  etolb <- chooseBoundedIntegral (round $ (0.9 :: Double) * fromIntegral hb, hb * 10)
  etoub <- chooseBoundedIntegral (max etolb (round $ (1.1 :: Double) * fromIntegral hb), 2 * etolb)
  faultProb <- case faultInjection of
    FaultInjection -> vectorOf (clusterSize + numLoneNodes) (elements [0.0, 0.001, 0.01])
    NoFaultInjection -> vectorOf (clusterSize + numLoneNodes) (pure 0)

  pSeed <- chooseBoundedIntegral (0, 1_000_000)
  pProb <- case faultInjection of
    FaultInjection -> elements [0.0, 0.2]
    NoFaultInjection -> pure 0
  loneWaits <-
    vectorOf
      numLoneNodes
      ( (,)
          <$> chooseBoundedIntegral (etoub, 3 * etoub)
          <*> chooseBoundedIntegral (etoub, 3 * etoub)
      )

  cmds <- replicateM numClients $ flip vectorOf (arbitrary @Command) =<< chooseBoundedIntegral (1, 15)
  pure $
    ScenarioInputs
      { heartbeatTimeout = hb,
        electionTimeoutLowerBound = etolb,
        electionTimeoutUpperBound = etoub,
        seeds = ss,
        faultProbabilities = faultProb,
        partitionSeed = pSeed,
        partitionProbability = pProb,
        numInitialClusterNodes = clusterSize,
        numInitialLoneNodes = numLoneNodes,
        numClientNodes = numClients,
        loneNodesWait = loneWaits,
        commands =
          IntMap.fromList $
            zip
              [0 .. numClients - 1]
              cmds
      }

data TestFault = TestFault deriving (Show)

instance Exception TestFault

data Server s
  = MkServer
  { sConfig :: Config Node,
    sSpec :: Implementation Command Node Result State (IOSim s),
    sFaultInjector :: TVar (IOSim s) (Maybe (ThreadId (IOSim s))) -> IOSim s ()
  }

runServerWithFaults :: Server s -> ClusterState Node -> IOSim s ()
runServerWithFaults server clusterState = do
  tidVar <- newTVarIO Nothing
  concurrently_
    (server.sFaultInjector tidVar)
    ( supervisor tidVar $
        runRaftServer
          server.sConfig
          clusterState
          mempty
          server.sSpec
    )
  where
    -- Restart a workload on failure, except if it returned
    -- normally.
    supervisor tidVar workload = do
      a <- async workload
      atomically $ writeTVar tidVar (Just (asyncThreadId a))
      r <- waitCatch a
      atomically $ writeTVar tidVar Nothing
      case r of
        Left _ -> do
          server.sSpec.tracer (Crashed server.sConfig.nodeId)
          threadDelay $ fromIntegral clientRetryTick -- simulate a restart
          supervisor tidVar workload
        Right () -> pure ()

-- | The pause before each client attempt, and the granularity in which the
-- retry budget below is spent.
clientRetryTick :: Microseconds
clientRetryTick = 10_000

-- | How long to wait for a resposne to a single request
clientRequestTimeout :: ScenarioInputs -> Microseconds
clientRequestTimeout inputs = 10 * inputs.electionTimeoutUpperBound

-- | How long the client keeps retrying one command before the scenario fails.
clientRetryBudget :: ScenarioInputs -> Microseconds
clientRetryBudget inputs = max 1_000_000 (3 * inputs.electionTimeoutUpperBound)

data MembershipDirection
  = Joining
  | Leaving
  deriving (Eq)

untilJoined,
  untilLeft ::
    -- | Run an admin action
    (forall a. RaftAdminT Node (IOSim s) a -> IOSim s a) ->
    -- | Leader if you know it, or initial contact
    Node ->
    -- | How long to wait for one admin request to be answered.
    Microseconds ->
    -- | How long to pause between attempts.
    Microseconds ->
    -- | The node whose membership should change
    Node ->
    -- | The command that requests the change
    RaftAdminT Node (IOSim s) r ->
    IOSim s ()
untilJoined = untilMembership Joining
untilLeft = untilMembership Leaving

untilMembership ::
  forall s r.
  MembershipDirection ->
  -- | Run an admin action
  (forall a. RaftAdminT Node (IOSim s) a -> IOSim s a) ->
  -- | Leader if you know it, or initial contact
  Node ->
  -- | How long to wait for one admin request to be answered.
  Microseconds ->
  -- | How long to pause between attempts.
  Microseconds ->
  -- | The node whose membership should change
  Node ->
  -- | The command that requests the change
  RaftAdminT Node (IOSim s) r ->
  IOSim s ()
untilMembership direction runAdminAction initialContact requestTimeout betweenAttempts node command = do
  go 4 initialContact
  where
    -- An admin action blocks until it gets a response, and the node being
    -- asked may well be down, so every attempt needs a bound.
    attempt :: forall a. RaftAdminT Node (IOSim s) a -> IOSim s (Maybe a)
    attempt = timeout (fromIntegral requestTimeout) . runAdminAction

    isDone = case direction of
      Joining -> Set.member node
      Leaving -> Set.notMember node

    go :: Int -> Node -> IOSim s ()
    go attemptsLeft contact
      | attemptsLeft <= 0 = pure ()
      | otherwise =
          attempt (Admin.getClusterConfiguration >>= Admin.sendAdminCommand contact) >>= \case
            -- Settled: the configuration says what we wanted it to say.
            Just (Right (ClusterConfigurationIs (Simple cluster)))
              | isDone cluster -> pure ()
            -- A settled configuration that is not the one we want. This is the
            -- only state in which asking for the change is the right move.
            Just (Right (ClusterConfigurationIs (Simple _))) -> attempt command >> waitAndRetry contact
            -- Only the leader answers a configuration read, so follow the
            -- redirect. This still costs an attempt, so a pair of nodes that
            -- disagree about who leads cannot keep us here forever.
            Just (Left (AdminNotLeader (Just leader)))
              | leader /= contact -> go (attemptsLeft - 1) leader
            -- Either a change is already in flight (a 'Joint' configuration) or
            -- nobody could tell us. Do not ask again on an unknown state: that
            -- is how a request gets issued after the change has already landed,
            -- and such a request has nothing left to happen for it. Wait and
            -- re-read instead.
            _ -> waitAndRetry contact
      where
        waitAndRetry c = do
          threadDelay (fromIntegral betweenAttempts)
          go (attemptsLeft - 1) c

type Mailbox s m = IntMap (TQueue (IOSim s) m)

type TopologyConstraints s = TVar (IOSim s) (Map Node (Set Node))

data NetworkFabric s
  = MkNetworkFabric
  { rpcMailbox :: Mailbox s (RPC Command Node State),
    rpcResultsMailbox :: Mailbox s (RPCResult Node Result),
    requestsMailbox :: Mailbox s (ClientRequest Command Node),
    responsesMailbox :: Mailbox s (ClientResponse Node Result),
    adminMailbox :: Mailbox s (AdminRequest Node),
    adminResponsesMailbox :: Mailbox s (AdminResponse Node),
    -- Simulating a network partition by prevent messages sent
    -- from given nodes to reach other nodes.
    --
    -- This design allows for asymmetric network partisions, since
    -- Map.fromList [A, Set.fromList [B]] means A can't send messages
    -- to B, but B  can send messages to A. Not clear if this is representative
    -- of something that is possible, but it comes for free.
    topologyConstraints :: TopologyConstraints s
  }

data Environment s
  = MkResources
  { networkFabric :: NetworkFabric s,
    logPersistence :: IntMap (TVar (IOSim s) (Map LogIndex (Term, LogEntry Command Node))),
    votePersistence :: IntMap (TVar (IOSim s) (Map Term Node)),
    termPersistence :: IntMap (TVar (IOSim s) Term),
    snapshotPersistence :: IntMap (TVar (IOSim s) (Maybe (Snapshot Node State))),
    commitIndexPersistence :: IntMap (TVar (IOSim s) LogIndex)
  }

newEnvironment :: Set Node -> IOSim s (Environment s)
newEnvironment nodes =
  MkResources
    <$> ( MkNetworkFabric
            <$> newMailbox
            <*> newMailbox
            <*> newMailbox
            <*> newMailbox
            <*> newMailbox
            <*> newMailbox
            <*> newTVarIO mempty -- no contraints at first
        )
    <*> newPersistence mempty
    <*> newPersistence mempty
    <*> newPersistence 0
    <*> newPersistence Nothing
    <*> newPersistence 0
  where
    newMailbox =
      IntMap.fromList
        <$> traverse (\n -> (fromIntegral n,) <$> newTQueueIO) (Set.toList nodes)
    newPersistence def =
      IntMap.fromList
        <$> traverse (\n -> (fromIntegral n,) <$> newTVarIO def) (Set.toList nodes)

mkServer :: (forall a. (Show a) => a -> a) -> Environment s -> Microseconds -> Microseconds -> Microseconds -> Double -> Word64 -> Node -> Server s
mkServer debug resources hbto etolb etoub faultProb seed node =
  MkServer
    { sConfig =
        MkConfig
          { nodeId = node,
            electionTimeoutRange = (etolb, etoub),
            heartBeatTimeout = hbto,
            randomSeed = seed,
            maxLogLength = Just 5 -- TODO: make configurable
          },
      sSpec =
        Implementation
          { persistence =
              Persistence
                { readLogEntriesFrom = readLogEntriesFromTest resources.logPersistence,
                  writeLogEntry = writeLogEntryTest resources.logPersistence,
                  truncateLogEntriesFrom = truncateLogEntriesTest resources.logPersistence,
                  readTerm = readTest resources.termPersistence,
                  writeTerm = writeTest resources.termPersistence,
                  readVotedFor = readVotedForTest resources.votePersistence,
                  writeVotedFor = writeVotedForTest resources.votePersistence,
                  readSnapshot = readTest resources.snapshotPersistence,
                  writeSnapshot = \self snapshot -> writeTest resources.snapshotPersistence self (Just snapshot),
                  readCommitIndex = readTest resources.commitIndexPersistence,
                  writeCommitIndex = writeTest resources.commitIndexPersistence
                },
            stateMachine = step,
            networking =
              Networking
                { sendRPC = send resources.networkFabric.rpcMailbox resources.networkFabric.topologyConstraints node,
                  sendRPCResult = send resources.networkFabric.rpcResultsMailbox resources.networkFabric.topologyConstraints node,
                  sendClientResponse = send resources.networkFabric.responsesMailbox resources.networkFabric.topologyConstraints node,
                  sendAdminResponse = send resources.networkFabric.adminResponsesMailbox resources.networkFabric.topologyConstraints node,
                  receiveRPC = receive resources.networkFabric.rpcMailbox node,
                  receiveRPCResult = receive resources.networkFabric.rpcResultsMailbox node,
                  receiveClientRequests = receiveAll resources.networkFabric.requestsMailbox node,
                  receiveAdminRequest = receive resources.networkFabric.adminMailbox node
                },
            -- We debug-print events here, rather than in `checkScenario`,
            -- because `checkScenario` can fail and produce no trace.
            tracer = traceM . debug
          },
      sFaultInjector = faultInjector faultProb seed
    }

faultInjector ::
  -- | Fault probability
  Double ->
  -- | Random seed
  Word64 ->
  TVar (IOSim s) (Maybe (ThreadId (IOSim s))) ->
  IOSim s ()
faultInjector faultProb seed tidVar = go (mkStdGen64 seed)
  where
    go gen = do
      threadDelay 10_000
      let (failProb, nextGen) = uniformR @Double (0, 1) gen
      tid <- atomically $ readTVar tidVar >>= maybe retry pure
      when (failProb <= faultProb) (throwTo tid TestFault)
      go nextGen

data Harness s
  = MkHarness
  { clusterServers :: IntMap (Server s),
    loneServers :: IntMap (Server s, MVar (IOSim s) (), Microseconds, Microseconds),
    hClients :: IntMap (ClientImplementation Command Node Result (IOSim s)),
    hAdminNode :: Node,
    hAdminSpec :: AdminImplementation Node (IOSim s)
  }

testHarness ::
  forall s.
  (forall a. (Show a) => a -> a) ->
  ScenarioInputs ->
  (Harness s -> IOSim s ()) ->
  IOSim s ()
testHarness
  debug
  (ScenarioInputs hb etlb etup s faultProbs pSeed pProb numClusterNodes numLoneNodes numClients loneWaits _commands)
  withHarness = do
    resources <- newEnvironment allNodes

    let mkServer' :: Double -> Word64 -> Node -> Server s
        mkServer' = mkServer debug resources hb etlb etup
    isDones <- replicateM numLoneNodes newEmptyMVar

    withAsync (networkPartitioner (networkFabric resources)) $ \_ ->
      withHarness
        MkHarness
          { clusterServers =
              IntMap.fromList $
                map (\(faultProb, serverSeed, n) -> (n, mkServer' faultProb serverSeed (fromIntegral n))) serverNodesWithMeta,
            loneServers =
              IntMap.fromList $
                zipWith
                  ( curry
                      ( \((faultProb, serverSeed, (waitBeforeJoin, waitBeforeLeave), n), isDone) ->
                          ( n,
                            ( mkServer' faultProb serverSeed (fromIntegral n),
                              isDone,
                              waitBeforeJoin,
                              waitBeforeLeave
                            )
                          )
                      )
                  )
                  loneNodesWithMeta
                  isDones,
            hClients = IntMap.fromList [(clientNode, clientSpec (networkFabric resources) (Node clientNode)) | clientNode <- clientNodes],
            hAdminNode = adminNode,
            hAdminSpec = adminSpec (networkFabric resources)
          }
    where
      allNodes =
        mconcat
          [ Set.fromList (fromIntegral <$> serverNodes),
            Set.fromList (fromIntegral <$> loneNodes),
            Set.fromList (fromIntegral <$> clientNodes),
            Set.singleton adminNode
          ]

      adminNode = -1

      serverNodes = [0 .. numClusterNodes - 1]
      serverNodesWithMeta = zip3 (take numClusterNodes faultProbs) (take numClusterNodes s) serverNodes

      loneNodes = [numClusterNodes .. numClusterNodes + numLoneNodes - 1]
      loneNodesWithMeta = zip4 (drop numClusterNodes faultProbs) (drop numClusterNodes s) loneWaits loneNodes

      -- Assuming there will never be 1M server nodes in these tests
      clientNodes =
        let start = numClusterNodes + numLoneNodes + 1
         in [start .. start + numClients - 1]

      clientSpec network clientNode =
        ClientImplementation
          { sendRequest = send network.requestsMailbox network.topologyConstraints clientNode,
            receiveResponse =
              receive network.responsesMailbox clientNode,
            Client.generateSessionId = \node -> do
              -- That's the simplest way I know how to introduce randomness in IOSim
              now <- getMonotonicTimeNSec
              pure $ sessionId now $ fromIntegral node
          }

      adminSpec network =
        AdminImplementation
          { sendAdminRequest = send network.adminMailbox network.topologyConstraints adminNode,
            receiveAdminResponse = receive network.adminResponsesMailbox adminNode,
            Admin.generateSessionId = pure . sessionId maxBound . fromIntegral
          }

      networkPartitioner network = go (mkStdGen64 pSeed)
        where
          go gen = do
            let (w, newGen) = uniformR (0, fromIntegral $ 10 * hb) gen
            threadDelay w
            let (constraintGraph, newGen') = randomGraph pProb allNodes newGen
            atomically $ writeTVar (topologyConstraints network) constraintGraph
            go newGen'

writeLogEntryTest ::
  (MonadSTM m) =>
  IntMap (TVar m (Map LogIndex (Term, LogEntry Command Node))) ->
  Node ->
  [(Term, LogIndex, LogEntry Command Node)] ->
  m ()
writeLogEntryTest storage self batch = case IntMap.lookup (fromIntegral self) storage of
  Nothing -> error $ "Persistence badly configured: missing node " <> show self
  Just var -> atomically $ do
    log' <- readTVar var
    -- Overwriting the log with new conflicting entries is important to ensure below
    writeTVar var (Map.fromList [(ix, (term, entry)) | (term, ix, entry) <- batch] <> log')

truncateLogEntriesTest ::
  (MonadSTM m) =>
  IntMap (TVar m (Map LogIndex (Term, LogEntry Command Node))) ->
  Node ->
  LogIndex ->
  m ()
truncateLogEntriesTest storage self truncateFrom = case IntMap.lookup (fromIntegral self) storage of
  Nothing -> error $ "Persistence badly configured: missing node " <> show self
  Just var -> atomically $ do
    log' <- readTVar var
    writeTVar var (log' `Map.restrictKeys` Set.fromAscList [0 .. pred truncateFrom])

readLogEntriesFromTest :: (MonadSTM m) => IntMap (TVar m (Map LogIndex (Term, LogEntry Command Node))) -> Node -> LogIndex -> m [(Term, LogIndex, LogEntry Command Node)]
readLogEntriesFromTest storage self logIndex = case IntMap.lookup (fromIntegral self) storage of
  Nothing -> error $ "Persistence badly configured: missing node " <> show self
  Just var -> readTVarIO var <&> fmap (\(ix, (term, entry)) -> (term, ix, entry)) . Map.toAscList . Map.filterWithKey (\ix _ -> ix >= logIndex)

writeVotedForTest :: (MonadSTM m) => IntMap (TVar m (Map Term Node)) -> Node -> Term -> Maybe Node -> m ()
writeVotedForTest storage self term value = case IntMap.lookup (fromIntegral self) storage of
  Nothing -> error $ "Persistence badly configured: missing node " <> show self
  Just var -> atomically $ do
    m <- readTVar var
    writeTVar var (Map.alter (const value) term m)

readVotedForTest :: (MonadSTM m) => IntMap (TVar m (Map Term Node)) -> Node -> Term -> m (Maybe Node)
readVotedForTest storage self term = case IntMap.lookup (fromIntegral self) storage of
  Nothing -> error $ "Persistence badly configured: missing node " <> show self
  Just var -> Map.lookup term <$> readTVarIO var

writeTest :: (MonadSTM m) => IntMap (TVar m a) -> Node -> a -> m ()
writeTest storage self value = case IntMap.lookup (fromIntegral self) storage of
  Nothing -> error $ "Persistence badly configured: missing node " <> show self
  Just var -> atomically $ writeTVar var value

readTest :: (MonadSTM m) => IntMap (TVar m a) -> Node -> m a
readTest storage self = case IntMap.lookup (fromIntegral self) storage of
  Nothing -> error $ "Persistence badly configured: missing node " <> show self
  Just var -> readTVarIO var

send ::
  (MonadSTM m) =>
  IntMap (TQueue m a) ->
  TVar m (Map Node (Set Node)) ->
  Node ->
  Node ->
  a ->
  m ()
send mailbox networkConstraints sender receipient message =
  case IntMap.lookup (fromIntegral receipient) mailbox of
    Nothing -> error $ "Mailbox badly configured: missing node " <> show receipient
    Just queue -> atomically $ do
      constraints <- readTVar networkConstraints
      unless (Set.member receipient $ Map.findWithDefault Set.empty sender constraints) $
        writeTQueue queue message

receive :: (MonadSTM m) => IntMap (TQueue m a) -> Node -> m a
receive mailbox node =
  case IntMap.lookup (fromIntegral node) mailbox of
    Nothing -> error $ "Mailbox badly configured: missing node " <> show node
    Just queue -> atomically $ readTQueue queue

receiveAll :: (MonadSTM m) => IntMap (TQueue m a) -> Node -> m (NonEmpty a)
receiveAll mailbox node =
  case IntMap.lookup (fromIntegral node) mailbox of
    Nothing -> error $ "Mailbox badly configures: missing node " <> show node
    Just queue ->
      atomically $
        flushTQueue queue <&> nonEmpty >>= \case
          Nothing -> retry
          Just xs -> pure xs

-- Random utilities

randomGraph ::
  (RandomGen g) =>
  -- | Probability of a node being selected in this graph
  Double ->
  Set Node ->
  g ->
  (Map Node (Set Node), g)
randomGraph p nodes gen =
  go gen candidates Map.empty
  where
    candidates =
      [ (src, dst)
      | src <- Set.toList nodes,
        dst <- Set.toList nodes,
        src /= dst
      ]

    go g [] graph = (graph, g)
    go g ((src, dst) : rest) graph =
      let (r, g') = randomR (0.0, 1.0) g
          graph'
            | r < p =
                Map.insertWith Set.union src (Set.singleton dst) graph
            | otherwise =
                graph
       in go g' rest graph'
