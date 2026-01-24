{-# LANGUAGE NumericUnderscores #-}

module Test.Network.Consensus.Raft (tests) where

import Control.Concurrent.Class.MonadSTM (atomically, modifyTVar', newTVarIO, readTVar, retry, writeTVar)
import Control.Monad.Class.MonadAsync (race_)
import Control.Monad.Class.MonadTimer (threadDelay)
import Control.Monad.IOSim (IOSim, runSimTrace, selectTraceEventsDynamic', traceM, traceResult)
import Data.ByteString (StrictByteString)
import Data.IntMap (IntMap)
import qualified Data.IntMap.Strict as IntMap
import Data.Sequence (Seq (..))
import qualified Data.Sequence as Seq
import Data.Set (Set)
import qualified Data.Set as Set
import Hedgehog
import qualified Hedgehog.Gen as Gen
import qualified Hedgehog.Range as Range
import Network.Consensus.Raft (Config (..), Microseconds (Microseconds), RaftSpec (..), RaftTrace (..), runRaftT, server)
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.Hedgehog (testProperty)

tests :: TestTree
tests =
  testGroup
    "Raft"
    [ testGroup
        "Unit tests"
        [ testSingleNodeElectsLeader
        ],
      testGroup "Property tests" []
    ]

testSingleNodeElectsLeader :: TestTree
testSingleNodeElectsLeader = testProperty "Single node cluster elects leader" $ property $ do
  seed <- forAll $ Gen.word64 (Range.linear minBound maxBound)
  -- If the heart beats are too frequent, the test cases take a very long time.
  -- By contrast, since elections are relatively infrequent, their
  -- timeouts can take on very small values
  heartbeatTimeout <- forAll $ Gen.int32 (Range.linear 1_000 200_000)
  electionTimeoutUpperBound <- forAll $ Gen.int32 (Range.linear 2 100_000)
  electionTimeoutLowerBound <- forAll $ Gen.int32 (Range.linear 1 electionTimeoutUpperBound)

  let config =
        MkConfig
          { nodeId = 0 :: Int,
            otherNodes = mempty,
            electionTimeoutRange = (Microseconds electionTimeoutLowerBound, Microseconds electionTimeoutUpperBound),
            heartBeatTimeout = Microseconds heartbeatTimeout,
            randomSeed = seed
          }
  let trace =
        runSimTrace $
          race_
            (threadDelay 1_000_000)
            ( do
                specs <- testSpecs (Set.singleton 0)
                case IntMap.lookup 0 specs of
                  Nothing -> pure ()
                  Just spec -> runRaftT config spec server
            )

      result = traceResult False trace

  case result of
    Left failed -> footnote (show failed) >> failure
    Right () ->
      selectTraceEventsDynamic' trace === [LeaderElected (0 :: Int)]

type Node = Int

testSpecs :: Set Node -> IOSim s (IntMap (RaftSpec entry Node result StrictByteString (IOSim s)))
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
          _serializeRPC = undefined,
          _serializeRPCResult = undefined,
          _deserializeRPC = undefined,
          _deserializeRPCResult = undefined,
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
