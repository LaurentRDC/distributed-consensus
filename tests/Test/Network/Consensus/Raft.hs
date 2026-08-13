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
  )
where

import Control.Concurrent.Class.MonadSTM (MonadSTM, TQueue, atomically, newTQueueIO, readTQueue, writeTQueue)
import Control.Monad (when)
import Control.Monad.Class.MonadAsync (concurrently_, forConcurrently_, withAsync)
import Control.Monad.Class.MonadTest (exploreRaces)
import Control.Monad.Class.MonadTimer (threadDelay, timeout)
import Control.Monad.IOSim (IOSim, exploreSimTrace, traceM)
import Control.Monad.Trans.Class (lift)
import qualified Data.Foldable as Foldable
import Data.Functor ((<&>))
import Data.IntMap (IntMap)
import qualified Data.IntMap.Strict as IntMap
import qualified Data.IntSet as IntSet
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.Maybe (fromMaybe)
import Data.Set (Set)
import qualified Data.Set as Set
import Data.Tagged (Tagged (..))
import qualified Data.Text.Lazy as Text
import Data.Word (Word64)
import Network.Consensus.Raft
  ( Config (..),
    Microseconds,
    RPC,
    RPCResult,
    RaftSpec (..),
    runRaftT,
  )
import qualified Network.Consensus.Raft as Raft
import Network.Consensus.Raft.Client (RaftClientSpec (..), RaftClientT, Request, Response, request, runRaftClientT)
import Test.Network.Consensus.Raft.Properties (allProperties)
import Test.Network.Consensus.Scenario (checkScenario)
import Test.Tasty (TestTree, askOption, localOption, testGroup)
import Test.Tasty.Options
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
  testProperty "Cluster properties without schedule exploration" $
    propClusterWith (pure ())

testClusterWithRaces :: TestTree
testClusterWithRaces =
  setNumRacyTests $
    testProperty "Cluster properties with schedule exploration" $
      propClusterWith exploreRaces

propClusterWith :: (forall s. IOSim s ()) -> Property
propClusterWith raceOrNot =
  property $
    forAll genScenarioInputs $
      \scenarioInputs ->
        -- Number of commands tuned for the test suite to take a few seconds.
        forAll (vectorOf 30 (arbitrary @Command)) $ \commands ->
          counterexample
            (Text.unpack $ pShow scenarioInputs)
            $ exploreSimTrace
              id
              ( scenario
                  scenarioInputs
                  commands
              )
              ( \_ trace ->
                  checkScenario
                    (allProperties @Command @Result @Node @State)
                    trace
              )
  where
    scenario scenarioInputs commands = do
      let stateMachineExpectations = expectedResults commands
      (harness :: Harness s) <- testHarness scenarioInputs

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
      withAsync (runServers harness) $ \_ ->
        runClient
          (runClientAction harness)
          -- In principle, there is no upper bound on how long a
          -- client can wait to receive a message. I'm putting a generous
          -- bound here because I have seen situations where the client never
          -- receives a reply (due to a bug)
          (10 * scenarioInputs.electionTimeoutUpperBound)
          stateMachineExpectations

    runServers :: Harness s -> IOSim s ()
    runServers harness =
      concurrently_
        -- Cluster nodes
        ( forConcurrently_ (IntMap.elems harness.clusterServers) $ \server ->
            runRaftT
              server.sConfig
              (Set.fromList (map fromIntegral $ IntMap.keys harness.clusterServers))
              mempty
              server.sSpec
              Raft.server
        )
        -- Lone nodes
        ( forConcurrently_ (IntMap.elems harness.loneServers) $ \(server, wait) ->
            runRaftT
              server.sConfig
              mempty -- lone nodes don't know about anyone
              mempty
              server.sSpec
              $ do
                lift (threadDelay (fromIntegral wait))
                maybe
                  (fail "No nodes in cluster to request membership")
                  (Raft.serverJoinCluster . fromIntegral . fst)
                  (IntSet.minView (IntMap.keysSet harness.clusterServers))
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
    loneNodesWait :: [Microseconds]
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
  loneWaits <- vectorOf numLoneNodes (chooseBoundedIntegral (etoub, 10 * etoub))
  pure $
    ScenarioInputs
      { heartbeatTimeout = hb,
        electionTimeoutLowerBound = etolb,
        electionTimeoutUpperBound = etoub,
        seeds = ss,
        numInitialClusterNodes = clusterSize,
        numInitialLoneNodes = numLoneNodes,
        loneNodesWait = loneWaits
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
    responsesMailbox :: Mailbox s (Response Node Result)
  }

newNetworkFabric :: Set Node -> IOSim s (NetworkFabric s)
newNetworkFabric nodes =
  MkNetworkFabric
    <$> newMailbox
    <*> newMailbox
    <*> newMailbox
    <*> newMailbox
  where
    newMailbox = IntMap.fromList <$> traverse (\n -> (fromIntegral n,) <$> newTQueueIO) (Set.toList nodes)

mkServer :: NetworkFabric s -> Microseconds -> Microseconds -> Microseconds -> Word64 -> Node -> Server s
mkServer networkFabric hbto etolb etoub seed node =
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
            _sendRPC = send networkFabric.rpcMailbox,
            _sendRPCResult = send networkFabric.rpcResultsMailbox,
            _sendClientResponse = send networkFabric.responsesMailbox,
            _receiveRPC = receive networkFabric.rpcMailbox node <&> Right,
            _receiveRPCResult = receive networkFabric.rpcResultsMailbox node <&> Right,
            _receiveClientRequest = receive networkFabric.requestsMailbox node <&> Right,
            _tracer = traceM
          }
    }

data Harness s
  = MkHarness
  { clusterServers :: IntMap (Server s),
    loneServers :: IntMap (Server s, Microseconds),
    -- TODO: have multiple concurrent clients
    runClientAction :: forall a. RaftClientT Command Node Result (IOSim s) a -> IOSim s a
  }

testHarness ::
  forall s.
  ScenarioInputs ->
  IOSim s (Harness s)
testHarness
  (ScenarioInputs hb etlb etup s numClusterNodes numLoneNodes loneWaits) = do
    networkFabric <- newNetworkFabric $ mconcat [Set.fromList (fromIntegral <$> serverNodes), Set.fromList (fromIntegral <$> loneNodes), Set.singleton clientNode]

    let mkServer' :: Word64 -> Node -> Server s
        mkServer' = mkServer networkFabric hb etlb etup

    pure $
      MkHarness
        { clusterServers =
            IntMap.fromList $
              map (\(serverSeed, n) -> (n, mkServer' serverSeed (fromIntegral n))) serverNodesWithSeeds,
          loneServers =
            IntMap.fromList $
              map
                (\(serverSeed, waitBeforeJoin, n) -> (n, (mkServer' serverSeed (fromIntegral n), waitBeforeJoin)))
                loneNodesWithSeedsAndWaits,
          runClientAction = \action -> runRaftClientT action clientNode (clientSpec networkFabric)
        }
    where
      clientNode = Node $ numClusterNodes + numLoneNodes + 1

      serverNodes = [0 .. numClusterNodes - 1]
      serverNodesWithSeeds = zip (take numClusterNodes s) serverNodes

      loneNodes = [numClusterNodes .. numLoneNodes - 1]
      loneNodesWithSeedsAndWaits = zip3 (drop numClusterNodes s) loneWaits loneNodes

      clientSpec networkFabric =
        MkRaftClientSpec
          { sendRequest = send networkFabric.requestsMailbox,
            receiveResponse =
              receive networkFabric.responsesMailbox clientNode <&> Right
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
    reverse $
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

  parseValue s =
    NumRacyTests . Just <$> safeRead s

  optionName = Tagged "num-racy-tests"

  optionHelp = Tagged "Number of racy tests to run"
