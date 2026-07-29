{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module Test.Network.Consensus.Raft (tests) where

import Control.Concurrent.Class.MonadSTM (atomically, modifyTVar', newTVarIO, readTVar, retry, writeTVar)
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
    runRaftT,
    server,
  )
import Network.Consensus.Raft.Client (RaftClientSpec (..), RaftClientT, Request (..), Response, request, runRaftClientT)
import Test.Network.Consensus.Raft.Properties (allProperties)
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
                  ( \_ trace ->
                      checkScenario
                        (allProperties @Entry @Result @Node)
                        trace
                  )
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
      MkHarness serverSpecs _clientSpec <- testHarness (Set.fromList $ IntMap.keys configs)
      -- The scenario must give enough time for potentially multiple elections to be triggered
      race_ (threadDelay (5 * fromIntegral electionTimeoutUpperBound)) $ do
        forConcurrently_ (IntMap.toList serverSpecs) $ \(ix, spec) ->
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
      (_, commandNode, MkCommand command _) <- eventually commandReceived <?> "Command received"
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
      MkHarness serverSpecs runClient <- testHarness (Set.fromList $ IntMap.keys configs)
      -- The scenario must give enough time for an election to be triggered,
      -- hence why we race against 2 * electionTimeoutUpperBound
      --
      -- Then, we send a single client request, from a fictitious node (we don't care about the
      -- client in this particular test)
      race_ (threadDelay (3 * fromIntegral electionTimeoutUpperBound)) $
        concurrently
          -- Normal cluster
          ( forConcurrently_ (IntMap.toList serverSpecs) $ \(ix, spec) ->
              runRaftT (configs IntMap.! ix) () spec server
          )
          -- Client request.
          -- If the cluster is in an election, the response will be "Left ...",
          -- and so we retry a little bit later.
          ( let f =
                  runClient (request 0 ()) >>= \case
                    Left _ -> threadDelay (fromIntegral heartbeatTimeout) >> f
                    Right r -> pure $ Right r
             in f
          )

type Node = Int

type Entry = ()

type Result = ()

type State = ()

data Harness s
  = MkHarness
  { serverSpecifications :: IntMap (RaftSpec Entry Node State Result TestMessage (IOSim s)),
    runClientAction :: forall a. RaftClientT Entry Node Result (IOSim s) a -> IOSim s a
  }

testHarness ::
  Set Node ->
  IOSim s (Harness s)
testHarness serverNodes = do
  mailbox <- newTVarIO mempty
  pure $
    MkHarness
      { serverSpecifications = IntMap.fromList [(node, serverSpec mailbox node) | node <- Set.toList serverNodes],
        runClientAction = \action -> runRaftClientT action clientNode (clientSpec mailbox)
      }
  where
    clientNode = maybe 0 succ (Set.lookupMax serverNodes)

    serverSpec mailbox node =
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
          _serializeClientResponse = TestClientResponse,
          _deserializeRPC = fromRPC,
          _deserializeRPCResult = fromRPCResult,
          _deserializeClientRequest = fromClientReq,
          _send = send mailbox,
          _receive = receive mailbox node,
          _tracer = traceM
        }

    clientSpec mailbox =
      MkRaftClientSpec
        { sendRequest = \n req -> send mailbox n (TestClientRequest req),
          receiveResponse =
            receive mailbox clientNode >>= \case
              TestClientResponse resp -> pure $ Right resp
              _ -> pure $ Left "Unexpected message type"
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
  = TestClientRequest (Request Node ())
  | TestClientResponse (Response Node ())
  | TestRPC (RPC Node ())
  | TestRPCResult (RPCResult Node ())
  deriving (Show)

fromClientReq :: TestMessage -> Either Text (Request Node ())
fromClientReq (TestClientRequest req) = Right req
fromClientReq _ = Left (Text.pack "unexpected failure")

fromRPC :: TestMessage -> Either Text (RPC Node ())
fromRPC (TestRPC rpc) = Right rpc
fromRPC _ = Left (Text.pack "unexpected failure")

fromRPCResult :: TestMessage -> Either Text (RPCResult Node ())
fromRPCResult (TestRPCResult result) = Right result
fromRPCResult _ = Left (Text.pack "Unexpected failure")
