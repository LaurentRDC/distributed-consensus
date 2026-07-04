{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Test.Network.Consensus.Raft (tests) where

import Control.Concurrent.Class.MonadSTM (atomically, modifyTVar', newTVarIO, readTVar, retry, writeTVar)
import Control.Monad ((>=>))
import Control.Monad.Class.MonadAsync (concurrently, forConcurrently_, race_)
import Control.Monad.Class.MonadTimer (threadDelay)
import Control.Monad.IOSim (IOSim, exploreSimTrace, traceM)
import Control.Monitor
import Data.IntMap (IntMap)
import qualified Data.IntMap.Strict as IntMap
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
import Test.Network.Consensus.Scenario (Scenario, checkScenario, commandReceived, commitIndexIncreased, leaderElected, logEntryApplied, rpcReceived)
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

testClusterElections :: TestTree
testClusterElections =
  testProperty "Cluster elects leader" $
    property $
      -- Only odd cluster sizes have an easy-to-clear quorum
      forAll (elements [1, 3, 5]) $ \clusterSize ->
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
                  (\_ trace -> checkScenario expectation trace)
  where
    expectation :: Scenario Entry Result Node
    expectation = whenever leaderElected $ \(term, _) ->
      never
        ( -- another leader elected for the same term
          \case
            LeaderElected t _ | t == term -> Just "Leader elected for the same term"
            _ -> Nothing
        )

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
      -- The scenario must give enough time for an election to be triggered,
      -- hence why we race against 2 * electionTimeoutUpperBound
      race_ (threadDelay (2 * fromIntegral electionTimeoutUpperBound)) $ do
        forConcurrently_ (IntMap.toList specs) $ \(ix, spec) ->
          runRaftT (configs IntMap.! ix) () spec server

testClusterProcessesCommands :: TestTree
testClusterProcessesCommands =
  testProperty "Cluster processes commands" $
    property $
      -- Only odd cluster sizes have an easy-to-clear quorum
      -- TODO: processing commands for a single-node cluster
      forAll (elements [3, 5]) $ \clusterSize ->
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
      (electionTerm, leaderNode) <- eventually leaderElected
      (_, commandNode, MkCommand _ command _) <- eventually commandReceived
      -- The command should end up at the leader
      expect
        (leaderNode == commandNode)
        ( "Unexpected command node: "
            <> show commandNode
            <> " instead of "
            <> show leaderNode
        )

      (appendEntries, (_, _, commitIndex)) <-
        collectUntil
          ( rpcReceived >=> \case
              (_, _, AE AppendEntries {}) -> Just ()
              _ -> Nothing
          )
          commitIndexIncreased

      let quorumSize = numNodes `div` 2 + 1
      expect (length appendEntries >= quorumSize) "Quorum not reached"
      expect (commitIndex == 1) "Unexpected commit index"

      (appliedTerm, appliedNode, entry) <- eventually logEntryApplied
      expect (appliedTerm == electionTerm) mempty
      expect (appliedNode == leaderNode) mempty
      expect (command == entry) mempty

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
