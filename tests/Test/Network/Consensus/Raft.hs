{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Test.Network.Consensus.Raft (tests) where

import Control.Concurrent.Class.MonadSTM (atomically, modifyTVar', newTVarIO, readTVar, retry, writeTVar)
import Control.Monad.Class.MonadAsync (forConcurrently_, race_)
import Control.Monad.Class.MonadTimer (threadDelay)
import Control.Monad.IOSim (IOSim, SimEvent, SimEventType (EventLog), Trace, exploreSimTrace, selectTraceEvents', traceM)
import Data.Dynamic (fromDynamic)
import Data.IntMap (IntMap)
import qualified Data.IntMap.Strict as IntMap
import Data.Sequence (Seq (..))
import qualified Data.Sequence as Seq
import Data.Set (Set)
import qualified Data.Set as Set
import Data.Text (Text)
import Network.Consensus.Raft
  ( Config (..),
    Microseconds (Microseconds),
    RPC,
    RPCResult,
    RaftSpec (..),
    RaftTrace (..),
    initialTerm,
    runRaftT,
    server,
  )
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.QuickCheck

tests :: TestTree
tests =
  testGroup
    "Raft"
    [ testGroup
        "Property tests"
        [ testClusterElections
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
                  ( \_ trace -> do
                      let evs = raftTrace trace
                          elections = [ev | ev@LeaderElected {} <- evs]
                      counterexample ("failed with trace: " ++ show evs) $
                        elections
                          `Set.member` Set.fromList
                            [[LeaderElected (initialTerm + 1) n] | n <- [0 .. length seeds - 1]]
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
      (specs :: IntMap (RaftSpec () Node () TestMessage (IOSim s))) <- testSpecs (Set.fromList $ IntMap.keys configs)
      -- The scenario must give enough time for an election to be triggered,
      -- hence why we race against 2 * electionTimeoutUpperBound
      race_ (threadDelay (2 * fromIntegral electionTimeoutUpperBound)) $ do
        forConcurrently_ (IntMap.toList specs) $ \(ix, spec) ->
          runRaftT (configs IntMap.! ix) spec server

raftTrace :: Trace a SimEvent -> [RaftTrace () Node]
raftTrace =
  selectTraceEvents'
    ( \_ ev -> case ev of
        EventLog dyn -> fromDynamic dyn
        _ -> Nothing -- internal io-sim event
    )

type Node = Int

type Entry = ()

type Result = ()

testSpecs ::
  Set Node -> IOSim s (IntMap (RaftSpec Entry Node Result TestMessage (IOSim s)))
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
          _applyLogEntry = \_ -> pure undefined,
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
        Just (next :<| rest) ->
          writeTVar mailbox (IntMap.insert node rest mail)
            >> pure next

data TestMessage
  = TestRPC (RPC Node ())
  | TestRPCResult (RPCResult Node ())

fromRPC :: TestMessage -> Either Text (RPC Node ())
fromRPC (TestRPC rpc) = Right rpc
fromRPC (TestRPCResult _) = Left "unexpected failure"

fromRPCResult :: TestMessage -> Either Text (RPCResult Node ())
fromRPCResult (TestRPCResult result) = Right result
fromRPCResult (TestRPC _) = Left "Unexpected failure"
