{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Test.Network.Consensus.Raft (tests) where

import Control.Concurrent.Class.MonadSTM (atomically, modifyTVar', newTVarIO, readTVar, retry, writeTVar)
import Control.Monad (void, when)
import Control.Monad.Class.MonadAsync (concurrently, forConcurrently_, race_)
import Control.Monad.Class.MonadTimer (threadDelay)
import Control.Monad.IOSim (IOSim, exploreSimTrace, traceM)
import Control.Monitor
import Data.IntMap (IntMap)
import qualified Data.IntMap.Strict as IntMap
import qualified Data.Map.Strict as Map
import Data.Sequence (Seq (..))
import qualified Data.Sequence as Seq
import Data.Set (Set)
import qualified Data.Set as Set
import Data.Text (Text)
import qualified Data.Text as Text
import Network.Consensus.Raft
  ( AppendEntries (..),
    Command (..),
    Config (..),
    Microseconds (Microseconds),
    RPC (..),
    RPCResult,
    RaftSpec (..),
    RaftTrace (..),
    runRaftT,
    server,
  )
import Test.Network.Consensus.Scenario (Scenario, checkScenario, commandReceived, commitIndexIncreased, leaderElected, logEntryApplied, rpcReceived, votedFor)
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.QuickCheck

tests :: TestTree
tests =
  testGroup
    "Raft"
    [ testGroup
        "Property tests"
        [ testClusterElections,
          testClusterProcessesCommands
        ]
    ]

-- | Ensure that in each term where a leader is elected, no other leader
-- is elected
electionMonitor :: Scenario Entry Result Node
electionMonitor = go
  where
    anotherLeaderIn term = predicate $ \case
      LeaderElected t _ | t == term -> Just ()
      _ -> Nothing

    -- Whenever a leader is elected, there should not be another
    -- leader during this term.
    --
    -- In order to allow the test to span multiple terms, we need to recursively
    -- apply the expectation using 'both'
    go = void $ whenever leaderElected $ \(term, _) ->
      both go (never (anotherLeaderIn term))
        <?> "Another leader elected for the same term"

-- | Ensure that a node casts at most one vote per term
singleVoteMonitor :: Scenario Entry Result Node
singleVoteMonitor = go mempty
  where
    go votes = void $ whenever votedFor $ \(voterTerm, voterNode, _, _) -> do
      when (Set.member (voterTerm, voterNode) votes) $ fail "Node cast more than one vote in term"

      go (Set.insert (voterTerm, voterNode) votes)

-- | Ensure that each node witnesses terms that increase monotonically
termMonitor :: (Ord node) => Scenario entry result node
termMonitor = go mempty
  where
    go latestKnownTerms = void $ do
      whenever (predicate roleTerm) $ \(node, newTerm) -> do
        case Map.lookup node latestKnownTerms of
          Nothing -> pure ()
          Just latestTerm ->
            assert
              "Expecting terms to increase monotonically"
              (latestTerm <= newTerm)
        go (Map.insert node newTerm latestKnownTerms)

    roleTerm (LeaderElected t n) = Just (n, t)
    roleTerm (BecameCandidate t n) = Just (n, t)
    roleTerm (BecameFollower t n) = Just (n, t)
    roleTerm _ = Nothing

testClusterElections :: TestTree
testClusterElections =
  testProperty "Cluster elects leader" $
    property $
      forAll (elements [1 .. 5]) $ \clusterSize ->
        -- The following timings ensure that once election happens,
        -- the heartbeat will be sent early enough to ensure the leader
        -- remains a leader.
        forAll (vectorOf clusterSize (chooseBoundedIntegral (0, 1_000_000))) $ \seeds ->
          forAll (chooseBoundedIntegral (1_000, 200_000)) $ \heartbeatTimeout ->
            forAll (chooseBoundedIntegral (heartbeatTimeout `div` 2, 2 * heartbeatTimeout)) $ \electionTimeoutLowerBound ->
              forAll (chooseBoundedIntegral (electionTimeoutLowerBound `div` 2, 2 * electionTimeoutLowerBound)) $ \electionTimeoutUpperBound ->
                exploreSimTrace
                  id
                  (scenario heartbeatTimeout electionTimeoutLowerBound electionTimeoutUpperBound (Set.fromList seeds))
                  (\_ trace -> checkScenario (void $ allOf [electionMonitor, singleVoteMonitor, termMonitor]) trace)
  where
    mkConfigs heartbeatTimeout electionTimeoutLowerBound electionTimeoutUpperBound seeds =
      let nodes = Set.fromList [0 .. length seeds - 1]
       in IntMap.fromList
            [ ( ix,
                MkConfig
                  { nodeId = ix,
                    otherNodes = nodes `Set.difference` Set.singleton ix,
                    electionTimeoutRange = (Microseconds electionTimeoutLowerBound, Microseconds electionTimeoutUpperBound),
                    heartBeatTimeout = Microseconds heartbeatTimeout,
                    randomSeed = seed
                  }
              )
            | (ix, seed) <- zip [0 ..] (Set.toList seeds)
            ]

    scenario heartbeatTimeout electionTimeoutLowerBound electionTimeoutUpperBound seeds = do
      let configs = mkConfigs heartbeatTimeout electionTimeoutLowerBound electionTimeoutUpperBound seeds
      (specs :: IntMap (RaftSpec Entry Node State Result TestMessage (IOSim s))) <- testSpecs (Set.fromList $ IntMap.keys configs)
      -- The scenario must give enough time for potentially multiple elections to be triggered
      race_ (threadDelay (5 * fromIntegral electionTimeoutUpperBound)) $ do
        forConcurrently_ (IntMap.toList specs) $ \(ix, spec) ->
          runRaftT (configs IntMap.! ix) () spec server

testClusterProcessesCommands :: TestTree
testClusterProcessesCommands =
  testProperty "Cluster processes commands" $
    property $
      forAll (elements [1 .. 5]) $ \clusterSize ->
        -- The following timings ensure that once election happens,
        -- the heartbeat will be sent early enough to ensure the leader
        -- remains a leader.
        -- TODO: play with timing to allow the possibility for an election to be triggered.
        --       How does one formulate a good property to check in this case?
        forAll (vectorOf clusterSize (chooseBoundedIntegral (0, 1_000_000))) $ \seeds ->
          forAll (chooseBoundedIntegral (1_000, 200_000)) $ \heartbeatTimeout ->
            forAll (chooseBoundedIntegral (heartbeatTimeout, 2 * heartbeatTimeout)) $ \electionTimeoutLowerBound ->
              forAll (chooseBoundedIntegral (electionTimeoutLowerBound, 2 * electionTimeoutLowerBound)) $ \electionTimeoutUpperBound ->
                exploreSimTrace
                  id
                  (scenario heartbeatTimeout electionTimeoutLowerBound electionTimeoutUpperBound (Set.fromList seeds))
                  (\_ trace -> checkScenario (expectation (length seeds)) trace)
  where
    mkConfigs heartbeatTimeout electionTimeoutLowerBound electionTimeoutUpperBound seeds =
      let nodes = Set.fromList [0 .. length seeds - 1]
       in IntMap.fromList
            [ ( ix,
                MkConfig
                  { nodeId = ix,
                    otherNodes = nodes `Set.difference` Set.singleton ix,
                    electionTimeoutRange = (Microseconds electionTimeoutLowerBound, Microseconds electionTimeoutUpperBound),
                    heartBeatTimeout = Microseconds heartbeatTimeout,
                    randomSeed = seed
                  }
              )
            | (ix, seed) <- zip [0 ..] (Set.toList seeds)
            ]

    expectation :: Int -> Scenario Entry Result Node
    expectation numNodes = do
      (electionTerm, leaderNode) <- eventually leaderElected <?> "Leader elected"
      (_, commandNode, MkCommand _ command _) <- eventually commandReceived <?> "Command received"
      -- The command should end up at the leader
      assert
        ( "Unexpected command node: "
            <> Text.show commandNode
            <> " instead of "
            <> Text.show leaderNode
        )
        (leaderNode == commandNode)

      (appendEntries, (_, _, commitIndex)) <-
        collectUntil
          ( do
              (_, _, AE AppendEntries {}) <- rpcReceived
              pure ()
          )
          commitIndexIncreased
          <?> "Enough entries appended until leader's commit index increased"

      let quorumSize = numNodes `div` 2 + 1
      assert "Quorum not reached" (length appendEntries >= pred quorumSize) -- 'pred' because we don't count the leader
      assert "Unexpected commit index" (commitIndex == 1)

      (appliedTerm, appliedNode, entry) <- eventually logEntryApplied <?> "Log entry applied"
      assert mempty (appliedTerm == electionTerm)
      assert mempty (appliedNode == leaderNode)
      assert mempty (command == entry)

    scenario heartbeatTimeout electionTimeoutLowerBound electionTimeoutUpperBound seeds = do
      let configs = mkConfigs heartbeatTimeout electionTimeoutLowerBound electionTimeoutUpperBound seeds
      (specs :: IntMap (RaftSpec Entry Node State Result TestMessage (IOSim s))) <- testSpecs (Set.fromList $ IntMap.keys configs)
      -- The scenario must give enough time for an election to be triggered,
      -- hence why we race against 2 * electionTimeoutUpperBound
      --
      -- Then, we send a single client request, from a fictitious node (we don't care about the
      -- client in this particular test)
      race_ (threadDelay (3 * fromIntegral electionTimeoutUpperBound)) $
        concurrently
          -- Normal cluster
          ( forConcurrently_ (IntMap.toList specs) $ \(ix, spec) ->
              runRaftT (configs IntMap.! ix) () spec server
          )
          -- Client request after a certain delay
          ( do
              threadDelay $ fromIntegral electionTimeoutUpperBound
              case IntMap.lookup 0 specs of
                Nothing -> error "Unexpected"
                Just spec -> _send spec 0 (TestRPC (ClientRequest (MkCommand 199_999 () 0)))
          )

type Node = Int

type Entry = ()

type Result = ()

type State = ()

testSpecs ::
  Set Node -> IOSim s (IntMap (RaftSpec Entry Node State Result TestMessage (IOSim s)))
testSpecs nodes = do
  mailbox <- newTVarIO mempty
  pure $ IntMap.fromList [(node, nodeSpec mailbox node) | node <- Set.toList nodes]
  where
    nodeSpec mailbox node =
      MkRaftSpec
        { _readLogEntry = \_ -> pure Nothing,
          _writeLogEntry = \_ _ _ -> pure (),
          _readTerm = pure 0,
          _writeTerm = \_ -> pure (),
          _readVotedFor = pure Nothing,
          _voteFor = \_ -> pure (),
          _applyLogEntry = \_ _ -> (() :: State, () :: Result),
          _serializeRPC = TestRPC,
          _serializeRPCResult = TestRPCResult,
          _deserializeRPC = fromRPC,
          _deserializeRPCResult = fromRPCResult,
          _send = send mailbox,
          _receive = receive mailbox node,
          _tracer = traceM
        }
    send mailbox node message =
      atomically $ modifyTVar' mailbox (IntMap.insertWith (<>) node (Seq.singleton message))

    receive mailbox node = atomically $ do
      mail <- readTVar mailbox
      case IntMap.lookup node mail of
        Nothing -> retry
        Just Seq.Empty -> retry
        Just (nextMessage :<| rest) ->
          writeTVar mailbox (IntMap.insert node rest mail)
            >> pure nextMessage

data TestMessage
  = TestRPC (RPC Node ())
  | TestRPCResult (RPCResult Node ())
  deriving (Show)

fromRPC :: TestMessage -> Either Text (RPC Node ())
fromRPC (TestRPC rpc) = Right rpc
fromRPC (TestRPCResult _) = Left (Text.pack "unexpected failure")

fromRPCResult :: TestMessage -> Either Text (RPCResult Node ())
fromRPCResult (TestRPCResult result) = Right result
fromRPCResult (TestRPC _) = Left (Text.pack "Unexpected failure")
