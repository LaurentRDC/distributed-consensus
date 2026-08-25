{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
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

import Control.Concurrent.Class.MonadMVar (MVar, newEmptyMVar, putMVar, takeMVar)
import Control.Concurrent.Class.MonadSTM (MonadSTM, TQueue, TVar, atomically, flushTQueue, newTQueueIO, newTVarIO, readTQueue, readTVar, readTVarIO, retry, writeTQueue, writeTVar)
import Control.Exception (Exception)
import Control.Monad (replicateM, when)
import Control.Monad.Class.MonadAsync (async, asyncThreadId, concurrently_, forConcurrently_, race_, waitCatch, withAsync)
import Control.Monad.Class.MonadFork (ThreadId, throwTo)
import Control.Monad.Class.MonadTest (exploreRaces)
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
    Microseconds,
    RPC,
    RPCResult,
    RaftTrace (..),
    Snapshot,
    Term,
    runRaftServer,
  )
import qualified Distributed.Consensus.Raft as Raft
import Distributed.Consensus.Raft.Admin
import qualified Distributed.Consensus.Raft.Admin as Admin
import Distributed.Consensus.Raft.Client (ClientRequest, ClientResponse, RaftClientSpec (..), RaftClientT, request, withRaftClientT)
import System.Random (mkStdGen64, uniformR)
import Test.Distributed.Consensus.Raft.Options (PrintTrace (..), setNumRacyTests, withExplorationOptions, withPrintTraceOption)
import Test.Distributed.Consensus.Raft.Properties (FaultInjection (..), allProperties)
import Test.Distributed.Consensus.Raft.Scenario (checkScenario)
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.QuickCheck
  ( Arbitrary (arbitrary),
    Gen,
    Property,
    Testable (property),
    chooseBoundedIntegral,
    chooseInt,
    counterexample,
    elements,
    forAll,
    oneof,
    testProperty,
    vectorOf,
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
                  (allProperties @Command @Result @Node @State faultInjection)
                  trace
            )
  where
    debug :: forall a. (Show a) => a -> a
    debug = case printTrace of
      PrintTrace False -> id
      PrintTrace True -> Debug.traceShowId
    scenario scenarioInputs = do
      (harness :: Harness s) <- testHarness debug scenarioInputs

      -- In order to detect infinite loops, especially in CI,
      -- we use a *very generous* scenario timeout.
      let timeStep = max clientRetryTick scenarioInputs.electionTimeoutUpperBound
          scenarioTimeUpperBound =
            10 * timeStep -- Baseline
            -- To ensure all lone nodes have time to join, and then leave
              + 3
                * maybe 0 maximum (nonEmpty $ sum <$> scenarioInputs.loneNodesWait)
              -- To ensure all client requests have time to be served
              + 3
                * genericLength scenarioInputs.commands
                * timeStep
              -- To ensure membership changes have time to land
              + 3
                * 6 -- 6 attempts
                * timeStep

      race_
        (threadDelay (fromIntegral scenarioTimeUpperBound) >> fail "Possible infinite loop detected")
        ( do
            withRaftAdminT harness.hAdminNode harness.hAdminSpec $ \runAdminAction ->
              -- Run servers until the clients are done interacting
              withAsync (runServers harness runAdminAction) $ \_ -> do
                -- Likewise, one client session for the whole command sequence,
                -- so that request IDs are unique across commands.
                withRaftClientT harness.hClientNode harness.hClientSpec $ \runClientAction ->
                  runClient
                    runClientAction
                    -- In principle, there is no upper bound on how long a
                    -- client can wait to receive a message. I'm putting a generous
                    -- bound here because I have seen situations where the client never
                    -- receives a reply (due to a bug)
                    (10 * scenarioInputs.electionTimeoutUpperBound)
                    (clientRetryBudget scenarioInputs)
                    mempty -- initial state
                    scenarioInputs.commands

                -- We give an opportunity to all lone servers to leave the cluster
                -- properly before we shut everyone down.
                let shutDownNode node =
                      let tryShutDown :: Int -> IOSim s ()
                          tryShutDown 0 = pure ()
                          tryShutDown attemptsLeft =
                            timeout
                              (fromIntegral scenarioInputs.electionTimeoutUpperBound)
                              (runAdminAction (Admin.shutDown node))
                              >>= maybe (tryShutDown (attemptsLeft - 1)) (const (pure ()))
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
                    Admin.joinCluster node contact

                  threadDelay (fromIntegral waitToLeave)
                  untilLeft runAdminAction contact requestTimeout betweenAttempts node $
                    Admin.leaveCluster node

                  putMVar isDone ()
              )
              (runServerWithFaults server Raft.LoneNode)
        )

    runClient ::
      (forall a. RaftClientT Command Node Result (IOSim s) a -> IOSim s a) ->
      -- \| Single request timeout
      Microseconds ->
      -- \| Timeout for retries
      Microseconds ->
      State ->
      [Command] ->
      IOSim s ()
    runClient runRequest maxTime retryBudget = go
      where
        go _ [] = pure ()
        go state (command : rest) = attempt retryBudget
          where
            (newState, expectedResults) = step state command

            attempt remaining = do
              threadDelay (fromIntegral clientRetryTick)
              timeout (fromIntegral maxTime) (runRequest (request 0 command)) >>= \case
                Nothing ->
                  giveUp $
                    "no response at all within "
                      <> show (toInteger maxTime)
                      <> " microseconds"
                -- Command needs to be re-tried
                Just (Left err)
                  | remaining <= clientRetryTick ->
                      giveUp $
                        "retried for "
                          <> show (toInteger retryBudget)
                          <> " microseconds, last response was "
                          <> show err
                  | otherwise -> attempt (remaining - clientRetryTick)
                Just (Right (_leaderId, actualResults)) -> do
                  when
                    (actualResults /= expectedResults)
                    ( fail $
                        "Unexpected state: following command "
                          <> show command
                          <> ", expecting "
                          <> show expectedResults
                          <> " but got "
                          <> show actualResults
                    )
                  go newState rest
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
                    FaultInjection -> pure ()
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
    numInitialClusterNodes :: Int,
    numInitialLoneNodes :: Int,
    loneNodesWait :: [(Microseconds, Microseconds)],
    commands :: [Command]
  }
  deriving (Eq, Show)

genScenarioInputs :: FaultInjection -> Gen ScenarioInputs
genScenarioInputs faultInjection = do
  clusterSize <- elements [1 .. 5]
  numLoneNodes <- elements [0 .. 2]
  ss <- vectorOf (clusterSize + numLoneNodes) (chooseBoundedIntegral (0, 1_000_000))
  hb <- chooseBoundedIntegral (clientRetryTick, 200_000)
  -- Making the lower election timeout possibly shorter than the heartbeat timeout
  -- allows to have terms with no leaders elected
  etolb <- chooseBoundedIntegral (round $ (0.9 :: Double) * fromIntegral hb, hb * 10)
  etoub <- chooseBoundedIntegral (etolb, 2 * etolb)
  faultProb <- case faultInjection of
    FaultInjection -> vectorOf (clusterSize + numLoneNodes) (elements [0.0, 0.001, 0.01])
    NoFaultInjection -> vectorOf (clusterSize + numLoneNodes) (pure 0)
  loneWaits <-
    vectorOf
      numLoneNodes
      ( (,)
          <$> chooseBoundedIntegral (etoub, 3 * etoub)
          <*> chooseBoundedIntegral (etoub, 3 * etoub)
      )

  cmds <- flip vectorOf (arbitrary @Command) =<< chooseBoundedIntegral (1, 20)
  pure $
    ScenarioInputs
      { heartbeatTimeout = hb,
        electionTimeoutLowerBound = etolb,
        electionTimeoutUpperBound = etoub,
        seeds = ss,
        faultProbabilities = faultProb,
        numInitialClusterNodes = clusterSize,
        numInitialLoneNodes = numLoneNodes,
        loneNodesWait = loneWaits,
        commands = cmds
      }

data TestFault = TestFault deriving (Show)

instance Exception TestFault

data Server s
  = MkServer
  { sConfig :: Config Node,
    sSpec :: Implementation Command Node State Result (IOSim s),
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
untilMembership direction runAdminAction initialContact requestTimeout betweenAttempts node command =
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
          attempt (Admin.getClusterConfiguration contact) >>= \case
            -- Settled: the configuration says what we wanted it to say.
            Just (Right (Simple cluster))
              | isDone cluster -> pure ()
            -- A settled configuration that is not the one we want. This is the
            -- only state in which asking for the change is the right move.
            Just (Right (Simple _)) -> attempt command >> waitAndRetry contact
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

data NetworkFabric s
  = MkNetworkFabric
  { rpcMailbox :: Mailbox s (RPC Node Command State),
    rpcResultsMailbox :: Mailbox s (RPCResult Node Result),
    requestsMailbox :: Mailbox s (ClientRequest Node Command),
    responsesMailbox :: Mailbox s (ClientResponse Node Result),
    adminMailbox :: Mailbox s (AdminRequest Node),
    adminResponsesMailbox :: Mailbox s (AdminResponse Node)
  }

data Environment s
  = MkResources
  { networkFabric :: NetworkFabric s,
    logPersistence :: IntMap (TVar (IOSim s) (Map LogIndex (Term, LogEntry Node Command))),
    votePersistence :: IntMap (TVar (IOSim s) (Map Term Node)),
    termPersistence :: IntMap (TVar (IOSim s) Term),
    snapshotPersistence :: IntMap (TVar (IOSim s) (Maybe (Snapshot Node State)))
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
        )
    <*> newPersistence mempty
    <*> newPersistence mempty
    <*> newPersistence 0
    <*> newPersistence Nothing
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
          { readLogEntry = readLogEntryTest resources.logPersistence,
            writeLogEntry = writeLogEntryTest resources.logPersistence,
            readTerm = readTest resources.termPersistence,
            writeTerm = writeTest resources.termPersistence,
            readVotedFor = readVotedForTest resources.votePersistence,
            writeVotedFor = writeVotedForTest resources.votePersistence,
            readSnapshot = readTest resources.snapshotPersistence,
            writeSnapshot = \self snapshot -> writeTest resources.snapshotPersistence self (Just snapshot),
            applyLogEntry = step,
            sendRPC = send resources.networkFabric.rpcMailbox,
            sendRPCResult = send resources.networkFabric.rpcResultsMailbox,
            sendClientResponse = send resources.networkFabric.responsesMailbox,
            sendAdminResponse = send resources.networkFabric.adminResponsesMailbox,
            receiveRPC = receive resources.networkFabric.rpcMailbox node <&> Right,
            receiveRPCResult = receive resources.networkFabric.rpcResultsMailbox node <&> Right,
            receiveClientRequests = receiveAll resources.networkFabric.requestsMailbox node,
            receiveAdminRequest = receive resources.networkFabric.adminMailbox node <&> Right,
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
    -- TODO: have multiple concurrent clients
    hClientNode :: Node,
    hClientSpec :: RaftClientSpec Command Node Result (IOSim s),
    hAdminNode :: Node,
    hAdminSpec :: RaftAdminSpec Node (IOSim s)
  }

testHarness ::
  forall s.
  (forall a. (Show a) => a -> a) ->
  ScenarioInputs ->
  IOSim s (Harness s)
testHarness
  debug
  (ScenarioInputs hb etlb etup s faultProbs numClusterNodes numLoneNodes loneWaits _commands) = do
    resources <-
      newEnvironment $
        mconcat
          [ Set.fromList (fromIntegral <$> serverNodes),
            Set.fromList (fromIntegral <$> loneNodes),
            Set.singleton clientNode,
            Set.singleton adminNode
          ]

    let mkServer' :: Double -> Word64 -> Node -> Server s
        mkServer' = mkServer debug resources hb etlb etup
    isDones <- replicateM numLoneNodes newEmptyMVar

    pure $
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
          hClientNode = clientNode,
          hClientSpec = clientSpec (networkFabric resources),
          hAdminNode = adminNode,
          hAdminSpec = adminSpec (networkFabric resources)
        }
    where
      adminNode = -1
      clientNode = Node $ numClusterNodes + numLoneNodes + 1

      serverNodes = [0 .. numClusterNodes - 1]
      serverNodesWithMeta = zip3 (take numClusterNodes faultProbs) (take numClusterNodes s) serverNodes

      loneNodes = [numClusterNodes .. numClusterNodes + numLoneNodes - 1]
      loneNodesWithMeta = zip4 (drop numClusterNodes faultProbs) (drop numClusterNodes s) loneWaits loneNodes

      clientSpec network =
        MkRaftClientSpec
          { sendRequest = send network.requestsMailbox,
            receiveResponse =
              receive network.responsesMailbox clientNode
          }

      adminSpec network =
        MkRaftAdminSpec
          { sendAdminRequest = send network.adminMailbox,
            receiveAdminResponse = receive network.adminResponsesMailbox adminNode <&> Right
          }

writeLogEntryTest ::
  (MonadSTM m) =>
  IntMap (TVar m (Map LogIndex (Term, LogEntry Node Command))) ->
  Node ->
  [(LogIndex, Term, LogEntry Node Command)] ->
  m ()
writeLogEntryTest storage self batch = case IntMap.lookup (fromIntegral self) storage of
  Nothing -> error $ "Persistence badly configured: missing node " <> show self
  Just var -> atomically $ do
    log' <- readTVar var
    writeTVar var (log' <> Map.fromList [(ix, (term, entry)) | (ix, term, entry) <- batch])

readLogEntryTest :: (MonadSTM m) => IntMap (TVar m (Map LogIndex (Term, LogEntry Node Command))) -> Node -> LogIndex -> m (Maybe (Term, LogEntry Node Command))
readLogEntryTest storage self logIndex = case IntMap.lookup (fromIntegral self) storage of
  Nothing -> error $ "Persistence badly configured: missing node " <> show self
  Just var -> readTVarIO var <&> Map.lookup logIndex

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

send :: (MonadSTM m) => IntMap (TQueue m a) -> Node -> a -> m ()
send mailbox node message =
  case IntMap.lookup (fromIntegral node) mailbox of
    Nothing -> error $ "Mailbox badly configured: missing node " <> show node
    Just queue -> atomically $ writeTQueue queue message

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

-- Simple key-value store

newtype Node = Node Int
  deriving (Eq, Show, Bounded, Enum, Num, Ord, Real, Integral)

type State = Map Char Int

data Command
  = Insert Char Int
  | Delete Char
  | Get Char
  deriving (Eq, Ord, Show)

instance Arbitrary Command where
  arbitrary =
    let possibleKeys = ['a', 'b', 'c']
     in oneof
          [ Insert <$> elements possibleKeys <*> chooseInt (0, 10),
            Delete <$> elements possibleKeys,
            Get <$> elements possibleKeys
          ]

data Result
  = Value Int
  | Ok
  | Err
  deriving (Eq, Show)

step :: State -> Command -> (State, Result)
step state (Insert k v) = (Map.insert k v state, Ok)
step state (Delete k) = (Map.delete k state, Ok)
step state (Get k) = case Map.lookup k state of
  Nothing -> (state, Err)
  Just v -> (state, Value v)
