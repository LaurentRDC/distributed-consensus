{-# LANGUAGE BangPatterns #-}
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
    NumRacyTests,
    PrintTrace,
  )
where

import Control.Concurrent.Class.MonadMVar (MVar, newEmptyMVar, putMVar, takeMVar)
import Control.Concurrent.Class.MonadSTM (MonadSTM, TQueue, atomically, newTQueueIO, readTQueue, writeTQueue)
import Control.Monad (replicateM, when)
import Control.Monad.Class.MonadAsync (concurrently_, forConcurrently_, race_, withAsync)
import Control.Monad.Class.MonadTest (exploreRaces)
import Control.Monad.Class.MonadTimer (threadDelay, timeout)
import Control.Monad.IOSim (IOSim, exploreSimTrace, traceM)
import qualified Data.Foldable as Foldable
import Data.Functor ((<&>))
import Data.IntMap (IntMap)
import qualified Data.IntMap.Strict as IntMap
import qualified Data.IntSet as IntSet
import Data.List (genericLength)
import Data.List.NonEmpty (nonEmpty)
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.Maybe (fromJust, fromMaybe)
import Data.Set (Set)
import qualified Data.Set as Set
import Data.Tagged (Tagged (..))
import qualified Data.Text.Lazy as Text
import Data.Word (Word64)
import qualified Debug.Trace as Debug
import Network.Consensus.Raft
  ( Config (..),
    Microseconds,
    RPC,
    RPCResult,
    RaftSpec (..),
    runRaftT,
  )
import qualified Network.Consensus.Raft as Raft
import Network.Consensus.Raft.Admin
import qualified Network.Consensus.Raft.Admin as Admin
import Network.Consensus.Raft.Client (RaftClientSpec (..), RaftClientT, Request, Response, request, runRaftClientT)
import Test.Network.Consensus.Raft.Properties (allProperties)
import Test.Network.Consensus.Scenario (checkScenario)
import Test.Tasty (TestTree, askOption, localOption, testGroup)
import Test.Tasty.Options
  ( IsOption (..),
    mkFlagCLParser,
    safeRead,
    safeReadBool,
  )
import Test.Tasty.QuickCheck
  ( Arbitrary (arbitrary),
    Gen,
    Property,
    QuickCheckTests (QuickCheckTests),
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
      propClusterWith printOrNot (pure ())

testClusterWithRaces :: TestTree
testClusterWithRaces =
  withPrintTraceOption $ \printOrNote ->
    setNumRacyTests $
      testProperty "Cluster properties with schedule exploration" $
        propClusterWith printOrNote exploreRaces

propClusterWith :: PrintTrace -> (forall s. IOSim s ()) -> Property
propClusterWith printTrace raceOrNot =
  property $
    forAll genScenarioInputs $
      \scenarioInputs ->
        counterexample
          (Text.unpack $ pShow scenarioInputs)
          $ exploreSimTrace
            id
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
      let stateMachineExpectations = expectedResults scenarioInputs.commands
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
                stateMachineExpectations

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
            runRaftT
              server.sConfig
              (Raft.InCluster $ Set.fromList (map fromIntegral $ IntMap.keys harness.clusterServers))
              mempty
              server.sSpec
              Raft.server
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
              ( runRaftT
                  server.sConfig
                  Raft.LoneNode
                  mempty
                  server.sSpec
                  Raft.server
              )
        )

    runClient ::
      (forall a. RaftClientT Command Node Result (IOSim s) a -> IOSim s a) ->
      Microseconds ->
      [(Command, State, Result)] ->
      IOSim s ()
    runClient _ _ [] = pure ()
    runClient runRequest maxTime ((command, state, expectedResult) : rest) = do
      threadDelay 10_000
      timeout (fromIntegral maxTime) (runRequest (request 0 command)) >>= \case
        Nothing -> fail $ "Client request timed out after " <> show (toInteger maxTime) <> " microseconds"
        -- Command needs to be re-tried
        Just (Left _) -> runClient runRequest maxTime ((command, state, expectedResult) : rest)
        Just (Right actualResult) -> do
          when (actualResult /= expectedResult) (fail "Unexpected state")
          runClient runRequest maxTime rest

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

data Server s
  = MkServer
  { sConfig :: Config Node,
    sSpec :: RaftSpec Command Node State Result (IOSim s)
  }

type Mailbox s m = (IntMap (TQueue (IOSim s) m))

data NetworkFabric s
  = MkNetworkFabric
  { rpcMailbox :: Mailbox s (RPC Node Command State),
    rpcResultsMailbox :: Mailbox s (RPCResult Node Result),
    requestsMailbox :: Mailbox s (Request Node Command),
    responsesMailbox :: Mailbox s (Response Node Result),
    adminMailbox :: Mailbox s (AdminRequest Node),
    adminResponsesMailbox :: Mailbox s (AdminResponse Node)
  }

newNetworkFabric :: Set Node -> IOSim s (NetworkFabric s)
newNetworkFabric nodes =
  MkNetworkFabric
    <$> newMailbox
    <*> newMailbox
    <*> newMailbox
    <*> newMailbox
    <*> newMailbox
    <*> newMailbox
  where
    newMailbox = IntMap.fromList <$> traverse (\n -> (fromIntegral n,) <$> newTQueueIO) (Set.toList nodes)

mkServer :: (forall a. (Show a) => a -> a) -> NetworkFabric s -> Microseconds -> Microseconds -> Microseconds -> Word64 -> Node -> Server s
mkServer debug network hbto etolb etoub seed node =
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
    networkFabric <-
      newNetworkFabric $
        mconcat
          [ Set.fromList (fromIntegral <$> serverNodes),
            Set.fromList (fromIntegral <$> loneNodes),
            Set.singleton clientNode,
            Set.singleton adminNode
          ]

    let mkServer' :: Word64 -> Node -> Server s
        mkServer' = mkServer debug networkFabric hb etlb etup

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
          runClientAction = \action -> runRaftClientT action clientNode (clientSpec networkFabric),
          runAdminAction = \action -> runRaftAdminT action adminNode (adminSpec networkFabric)
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
              receive network.responsesMailbox clientNode <&> Right
          }

      adminSpec network =
        MkRaftAdminSpec
          { sendAdminRequest = send network.adminMailbox,
            receiveAdminResponse = receive network.adminResponsesMailbox adminNode <&> Right
          }

send :: (MonadSTM m) => IntMap (TQueue m a) -> Node -> a -> m ()
send mailbox node message =
  case IntMap.lookup (fromIntegral node) mailbox of
    Nothing -> error $ "Mailbox badly configures: missing node " <> show node
    Just queue -> atomically $ writeTQueue queue message

receive :: (MonadSTM m) => IntMap (TQueue m a) -> Node -> m a
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

expectedResults :: [Command] -> [(Command, State, Result)]
expectedResults [] = []
expectedResults allCommands@(cmd : cmds) =
  zipWith (\c (s, r) -> (c, s, r)) allCommands $
    -- Using foldl' qualified to prevent
    -- warning of foldl' already being in prelude
    -- since GHC 9.10
    reverse $
      -- Using foldl' qualified to prevent
      -- warning of foldl' already being in prelude
      -- since GHC 9.10
      snd $
        -- Using foldl' qualified to prevent
        -- warning of foldl' already being in prelude
        -- since GHC 9.10
        Foldable.foldl'
          ( \(state, results) c ->
              let (!newState, !result) = step state c in (newState, (state, result) : results)
          )
          (let (state, result) = step mempty cmd in (state, [(state, result)]))
          cmds

setNumRacyTests :: TestTree -> TestTree
setNumRacyTests tree =
  -- These tests are potentially very long. We want a small default (here, 3),
  -- but with the ability to set it to a larger or smaller number at weill.
  --
  -- 'QuickCheckTests' doesn't allow this, as its default is 100, which is much
  -- too large
  askOption $ \(NumRacyTests n) ->
    let numTests = fromMaybe 3 n
     in localOption (QuickCheckTests numTests) tree

newtype NumRacyTests
  = NumRacyTests (Maybe Int)
  deriving (Eq, Ord, Show)

instance IsOption NumRacyTests where
  defaultValue = NumRacyTests Nothing
  parseValue s = NumRacyTests . Just <$> safeRead s
  optionName = Tagged "num-racy-tests"
  optionHelp = Tagged "Number of racy tests to run"

withPrintTraceOption :: (PrintTrace -> TestTree) -> TestTree
withPrintTraceOption = askOption

newtype PrintTrace
  = PrintTrace Bool
  deriving (Eq, Ord, Show)

instance IsOption PrintTrace where
  defaultValue = PrintTrace False
  optionName = Tagged "print-trace"
  parseValue = fmap PrintTrace . safeReadBool
  optionHelp = Tagged "Print the execution trace. This is generally only useful for a specific test replay."
  optionCLParser = mkFlagCLParser mempty (PrintTrace True)
