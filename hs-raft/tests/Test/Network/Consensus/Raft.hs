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

module Test.Network.Consensus.Raft
  ( tests,
  )
where

import Control.Concurrent.Class.MonadMVar (MVar, newEmptyMVar, putMVar, takeMVar)
import Control.Concurrent.Class.MonadSTM (MonadSTM, TQueue, TVar, atomically, flushTQueue, newTQueueIO, newTVarIO, readTQueue, readTVar, readTVarIO, retry, writeTQueue, writeTVar)
import Control.Monad (replicateM, when, (>=>))
import Control.Monad.Class.MonadAsync (concurrently_, forConcurrently_, race_, waitCatch, withAsync)
import Control.Monad.Class.MonadTest (exploreRaces)
import Control.Monad.Class.MonadTimer (threadDelay, timeout)
import Control.Monad.IOSim (ExplorationOptions, IOSim, exploreSimTrace, traceM)
import Data.Functor ((<&>))
import Data.IntMap (IntMap)
import qualified Data.IntMap.Strict as IntMap
import qualified Data.IntSet as IntSet
import Data.List (genericLength)
import Data.List.NonEmpty (NonEmpty, nonEmpty)
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.Maybe (fromJust)
import Data.Set (Set)
import qualified Data.Set as Set
import qualified Data.Text.Lazy as Text
import Data.Word (Word64)
import qualified Debug.Trace as Debug
import Network.Consensus.Raft
  ( Config (..),
    LogIndex,
    Microseconds,
    RPC,
    RPCResult,
    RaftSpec (..),
    Snapshot,
    Term,
    runRaftServer,
  )
import qualified Network.Consensus.Raft as Raft
import Network.Consensus.Raft.Admin
import qualified Network.Consensus.Raft.Admin as Admin
import Network.Consensus.Raft.Client (ClientRequest, ClientResponse, RaftClientSpec (..), RaftClientT, request, runRaftClientT)
import Test.Network.Consensus.Raft.Options (PrintTrace (..), setNumRacyTests, withExplorationOptions, withPrintTraceOption)
import Test.Network.Consensus.Raft.Properties (allProperties)
import Test.Network.Consensus.Scenario (checkScenario)
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
        [ testClusterWithoutRaces,
          testClusterWithRaces
        ]
    ]

testClusterWithoutRaces :: TestTree
testClusterWithoutRaces =
  withPrintTraceOption $ \printOrNot ->
    testProperty "Cluster properties without schedule exploration" $
      propClusterWith
        printOrNot
        id -- exploration options don't apply to non-racy simulations
        (pure ())

testClusterWithRaces :: TestTree
testClusterWithRaces =
  withPrintTraceOption $ \printOrNote ->
    setNumRacyTests $
      withExplorationOptions $ \updateExplorationOptions ->
        testProperty "Cluster properties with schedule exploration" $
          propClusterWith printOrNote updateExplorationOptions exploreRaces

propClusterWith ::
  PrintTrace ->
  (ExplorationOptions -> ExplorationOptions) ->
  (forall s. IOSim s ()) ->
  Property
propClusterWith printTrace updateExplorationOptions raceOrNot =
  property $
    forAll genScenarioInputs $
      \scenarioInputs ->
        counterexample
          (Text.unpack $ pShow scenarioInputs)
          $ exploreSimTrace
            updateExplorationOptions
            (scenario scenarioInputs)
            ( \_ trace ->
                checkScenario
                  (allProperties @Command @Result @Node @State)
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
      let timeStep = max 10_000 scenarioInputs.electionTimeoutUpperBound
          scenarioTimeUpperBound =
            10 * timeStep -- Baseline
            -- To ensure all lone nodes have time to join, and then leave
              + 3
                * maybe 0 maximum (nonEmpty $ sum <$> scenarioInputs.loneNodesWait)
              -- To ensure all client requests have time to be served
              + 3
                * genericLength scenarioInputs.commands
                * timeStep

      race_
        (threadDelay (fromIntegral scenarioTimeUpperBound) >> fail "Possible infinite loop detected")
        ( do
            -- This is the point where we can mark this thread "racy"
            -- (by default, it is not).
            --
            -- The benefit of NOT marking this racy is to explore many more of the initial
            -- parameter space (more 'ScenarioInputs's).
            --
            -- The benefit of marking this racy is to explore races within fewer initial
            -- conditions
            raceOrNot

            -- Run servers until the clients are done interacting
            withAsync (runServers harness) $ \_ -> do
              runClient
                (runClientAction harness)
                -- In principle, there is no upper bound on how long a
                -- client can wait to receive a message. I'm putting a generous
                -- bound here because I have seen situations where the client never
                -- receives a reply (due to a bug)
                (10 * scenarioInputs.electionTimeoutUpperBound)
                mempty -- initial state
                scenarioInputs.commands

              -- We give an opportunity to all lone servers to leave the cluster
              -- properly before we shut everyone down.
              --
              -- This makes it easier to specify properties
              forConcurrently_ harness.loneServers $ \(server, isDone, _, _) -> do
                takeMVar isDone
                runAdminAction harness (Admin.shutDown server.sConfig.nodeId)
              forConcurrently_ harness.clusterServers $ \server ->
                runAdminAction harness (Admin.shutDown server.sConfig.nodeId)
        )
    runServers :: Harness s -> IOSim s ()
    runServers harness =
      concurrently_
        -- Cluster nodes
        ( forConcurrently_ (IntMap.elems harness.clusterServers) $ \server ->
            supervise $
              runRaftServer
                server.sConfig
                (Raft.InCluster $ Set.fromList (map fromIntegral $ IntMap.keys harness.clusterServers))
                mempty
                server.sSpec
        )
        -- Lone nodes
        ( forConcurrently_ (IntMap.elems harness.loneServers) $ \(server, isDone, waitToJoin, waitToLeave) ->
            concurrently_
              ( threadDelay (fromIntegral waitToJoin)
                  >> runAdminAction
                    harness
                    ( Admin.joinCluster
                        server.sConfig.nodeId
                        ( fromIntegral $
                            fst $
                              fromJust $
                                IntSet.minView (IntMap.keysSet harness.clusterServers)
                        )
                    )
                  >> threadDelay (fromIntegral waitToLeave)
                  >> runAdminAction
                    harness
                    (Admin.leaveCluster server.sConfig.nodeId)
                  -- We give some time for nodes to actually leave.

                  >> threadDelay 100_000
                  >> putMVar isDone ()
              )
              ( supervise $
                  runRaftServer
                    server.sConfig
                    Raft.LoneNode
                    mempty
                    server.sSpec
              )
        )

    runClient ::
      (forall a. RaftClientT Command Node Result (IOSim s) a -> IOSim s a) ->
      Microseconds ->
      State ->
      [Command] ->
      IOSim s ()
    runClient _ _ _ [] = pure ()
    runClient runRequest maxTime state (command : rest) = do
      threadDelay 10_000
      let (newState, expectedResults) = step state command
      timeout (fromIntegral maxTime) (runRequest (request 0 command)) >>= \case
        Nothing -> fail $ "Client request timed out after " <> show (toInteger maxTime) <> " microseconds"
        -- Command needs to be re-tried
        Just (Left _) -> runClient runRequest maxTime state (command : rest)
        Just (Right (_leaderId, actualResults)) -> do
          when (actualResults /= expectedResults) (fail "Unexpected state")
          runClient runRequest maxTime newState rest

data ScenarioInputs
  = ScenarioInputs
  { heartbeatTimeout :: Microseconds,
    electionTimeoutLowerBound :: Microseconds,
    electionTimeoutUpperBound :: Microseconds,
    seeds :: [Word64],
    numInitialClusterNodes :: Int,
    numInitialLoneNodes :: Int,
    loneNodesWait :: [(Microseconds, Microseconds)],
    commands :: [Command]
  }
  deriving (Eq, Show)

genScenarioInputs :: Gen ScenarioInputs
genScenarioInputs = do
  clusterSize <- elements [1 .. 5]
  numLoneNodes <- elements [0 .. 2]
  ss <- vectorOf (clusterSize + numLoneNodes) (chooseBoundedIntegral (0, 1_000_000))
  hb <- chooseBoundedIntegral (1_000, 200_000)
  -- Making the lower election timeout possibly shorter than the heartbeat timeout
  -- allows to have terms with no leaders elected
  etolb <- chooseBoundedIntegral (round $ (0.9 :: Double) * fromIntegral hb, hb * 10)
  etoub <- chooseBoundedIntegral (etolb, 2 * etolb)
  loneWaits <-
    vectorOf
      numLoneNodes
      ( (,)
          <$> chooseBoundedIntegral (etoub, 10 * etoub)
          <*> chooseBoundedIntegral (etoub, 10 * etoub)
      )

  cmds <- flip vectorOf (arbitrary @Command) =<< chooseBoundedIntegral (1, 30)
  pure $
    ScenarioInputs
      { heartbeatTimeout = hb,
        electionTimeoutLowerBound = etolb,
        electionTimeoutUpperBound = etoub,
        seeds = ss,
        numInitialClusterNodes = clusterSize,
        numInitialLoneNodes = numLoneNodes,
        loneNodesWait = loneWaits,
        commands = cmds
      }

-- | Take the given process, and run it in a separate thread.
--
-- If the process exits due to an exception, restart it; otherwise,
-- let it end.
supervise :: IOSim s () -> IOSim s ()
supervise f =
  withAsync f (waitCatch >=> either (const (supervise f)) pure)

data Server s
  = MkServer
  { sConfig :: Config Node,
    sSpec :: RaftSpec Command Node State Result (IOSim s)
  }

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

data Resources s
  = MkResources
  { networkFabric :: NetworkFabric s,
    logPersistence :: IntMap (TVar (IOSim s) (Map LogIndex (Term, Command))),
    votePersistence :: IntMap (TVar (IOSim s) (Maybe Node)),
    termPersistence :: IntMap (TVar (IOSim s) Term),
    snapshotPersistence :: IntMap (TVar (IOSim s) (Maybe (Snapshot Node State)))
  }

newResources :: Set Node -> IOSim s (Resources s)
newResources nodes =
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
    <*> newPersistence Nothing
    <*> newPersistence 0
    <*> newPersistence Nothing
  where
    newMailbox =
      IntMap.fromList
        <$> traverse (\n -> (fromIntegral n,) <$> newTQueueIO) (Set.toList nodes)
    newPersistence def =
      IntMap.fromList
        <$> traverse (\n -> (fromIntegral n,) <$> newTVarIO def) (Set.toList nodes)

mkServer :: (forall a. (Show a) => a -> a) -> Resources s -> Microseconds -> Microseconds -> Microseconds -> Word64 -> Node -> Server s
mkServer debug resources hbto etolb etoub seed node =
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
        MkRaftSpec
          { _readLogEntry = readLogEntry resources.logPersistence,
            _writeLogEntry = writeLogEntry resources.logPersistence,
            _readTerm = read resources.termPersistence,
            _writeTerm = write resources.termPersistence,
            _readVotedFor = read resources.votePersistence,
            _voteFor = write resources.votePersistence,
            _readSnapshot = read resources.snapshotPersistence,
            _writeSnapshot = \self snapshot -> write resources.snapshotPersistence self (Just snapshot),
            _applyLogEntry = step,
            _sendRPC = send resources.networkFabric.rpcMailbox,
            _sendRPCResult = send resources.networkFabric.rpcResultsMailbox,
            _sendClientResponse = send resources.networkFabric.responsesMailbox,
            _sendAdminResponse = send resources.networkFabric.adminResponsesMailbox,
            _receiveRPC = receive resources.networkFabric.rpcMailbox node <&> Right,
            _receiveRPCResult = receive resources.networkFabric.rpcResultsMailbox node <&> Right,
            _receiveClientRequests = receiveAll resources.networkFabric.requestsMailbox node,
            _receiveAdminRequest = receive resources.networkFabric.adminMailbox node <&> Right,
            -- We debug-print events here, rather than in `checkScenario`,
            -- because `checkScenario` can fail and produce no trace.
            _tracer = traceM . debug
          }
    }

data Harness s
  = MkHarness
  { clusterServers :: IntMap (Server s),
    loneServers :: IntMap (Server s, MVar (IOSim s) (), Microseconds, Microseconds),
    -- TODO: have multiple concurrent clients
    runClientAction :: forall a. RaftClientT Command Node Result (IOSim s) a -> IOSim s a,
    runAdminAction :: forall a. RaftAdminT Node (IOSim s) a -> IOSim s a
  }

testHarness ::
  forall s.
  (forall a. (Show a) => a -> a) ->
  ScenarioInputs ->
  IOSim s (Harness s)
testHarness
  debug
  (ScenarioInputs hb etlb etup s numClusterNodes numLoneNodes loneWaits _commands) = do
    resources <-
      newResources $
        mconcat
          [ Set.fromList (fromIntegral <$> serverNodes),
            Set.fromList (fromIntegral <$> loneNodes),
            Set.singleton clientNode,
            Set.singleton adminNode
          ]

    let mkServer' :: Word64 -> Node -> Server s
        mkServer' = mkServer debug resources hb etlb etup

    isDones <- replicateM numLoneNodes newEmptyMVar

    pure $
      MkHarness
        { clusterServers =
            IntMap.fromList $
              map (\(serverSeed, n) -> (n, mkServer' serverSeed (fromIntegral n))) serverNodesWithSeeds,
          loneServers =
            IntMap.fromList $
              zipWith
                ( curry
                    ( \((serverSeed, (waitBeforeJoin, waitBeforeLeave), n), isDone) ->
                        ( n,
                          ( mkServer' serverSeed (fromIntegral n),
                            isDone,
                            waitBeforeJoin,
                            waitBeforeLeave
                          )
                        )
                    )
                )
                loneNodesWithSeedsAndWaits
                isDones,
          runClientAction = \action -> runRaftClientT action clientNode (clientSpec (networkFabric resources)),
          runAdminAction = \action -> runRaftAdminT action adminNode (adminSpec (networkFabric resources))
        }
    where
      adminNode = -1
      clientNode = Node $ numClusterNodes + numLoneNodes + 1

      serverNodes = [0 .. numClusterNodes - 1]
      serverNodesWithSeeds = zip (take numClusterNodes s) serverNodes

      loneNodes = [numClusterNodes .. numLoneNodes - 1]
      loneNodesWithSeedsAndWaits = zip3 (drop numClusterNodes s) loneWaits loneNodes

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

writeLogEntry ::
  (MonadSTM m) =>
  IntMap (TVar m (Map LogIndex (Term, Command))) ->
  Node ->
  LogIndex ->
  Term ->
  Command ->
  m ()
writeLogEntry storage self logIndex term entry = case IntMap.lookup (fromIntegral self) storage of
  Nothing -> error $ "Persistence badly configured: missing node " <> show self
  Just var -> atomically $ do
    log' <- readTVar var
    writeTVar var (Map.insert logIndex (term, entry) log')

readLogEntry :: (MonadSTM m) => IntMap (TVar m (Map LogIndex (Term, Command))) -> Node -> LogIndex -> m (Maybe Command)
readLogEntry storage self logIndex = case IntMap.lookup (fromIntegral self) storage of
  Nothing -> error $ "Persistence badly configured: missing node " <> show self
  Just var -> readTVarIO var <&> fmap snd . Map.lookup logIndex

write :: (MonadSTM m) => IntMap (TVar m a) -> Node -> a -> m ()
write storage self value = case IntMap.lookup (fromIntegral self) storage of
  Nothing -> error $ "Persistence badly configured: missing node " <> show self
  Just var -> atomically $ writeTVar var value

read :: (MonadSTM m) => IntMap (TVar m a) -> Node -> m a
read storage self = case IntMap.lookup (fromIntegral self) storage of
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
