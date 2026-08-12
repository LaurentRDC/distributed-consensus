{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module Test.Network.Consensus.Scenario
  ( Scenario,
    checkScenario,
    module Control.Monitor,

    -- * Injecting faults
    faultInjector,

    -- * Helpers
    leaderElected,
    votedFor,
    commandReceived,
    commandResponded,
    clusterMembershipChangeInitiated,
    clusterMembershipChangeCompleted,
    commitIndexIncreased,
    logEntryApplied,
    rpcReceived,
    rpcResultReceived,
  )
where

import Control.Concurrent.Class.MonadMVar (MonadMVar, modifyMVarMasked_, newMVar, readMVar)
import Control.Exception (Exception)
import Control.Monad.Class.MonadAsync (MonadAsync, forConcurrently_, race_)
import Control.Monad.Class.MonadFork (MonadFork, myThreadId, throwTo)
import Control.Monad.Class.MonadThrow (MonadThrow)
import Control.Monad.Class.MonadTimer (MonadDelay (..))
import Control.Monad.IOSim (SimEvent, SimEventType (EventLog), SimResult, Trace, selectTraceEvents)
import Control.Monitor
import Data.Dynamic (Typeable, fromDynamic)
import qualified Data.Text as Text
import Data.Word (Word64)
import Network.Consensus.Raft (Command (..), CommandResponse, Event (..), LogIndex, RPC (..), RPCResult, RaftTrace (..), Term)
import System.Random.Stateful (mkStdGen64, uniformR, uniformShuffleList)
import Test.Tasty.QuickCheck

type Scenario entry result state node =
  Monitor (RaftTrace entry result state node) ()

-- | Check a scenario over a 'Trace'.
checkScenario ::
  ( Show entry,
    Show result,
    Show node,
    Show state,
    Typeable entry,
    Typeable result,
    Typeable node,
    Typeable state
  ) =>
  Scenario entry result state node ->
  Trace (SimResult a) SimEvent ->
  Property
checkScenario scenario trace' =
  let evs = raftTrace trace'
   in case runMonitor scenario evs of
        (Left errs) -> counterexample (Text.unpack $ ppReasonsWithTrace Text.show 3 (zip [0 ..] evs) errs) False
        -- There have been instances in the past of tests trivially passing
        -- because no events were emitted!
        (Right _) -> counterexample "No events emitted" (not (null evs))

data TestFault = TestFault
  deriving (Show)

instance Exception TestFault

-- | Given a bunch of threads, inject crashes in said threads
-- TODO: exceptions thrown from this function don't appear to register, despite
-- inserting 'yield' basically everywhere...
faultInjector ::
  (MonadThrow m, MonadMVar m, MonadAsync m, MonadDelay m, MonadFork m) =>
  -- | Run function, which will be executed in parallel
  (a -> m ()) ->
  [a] ->
  -- | Probability of a crash in any thread, every 10ms
  -- This number will be clamped to the [0, 1] range.
  Double ->
  Word64 ->
  m ()
faultInjector f initials faultProbability seed = do
  threadIds <- newMVar []

  let gen = mkStdGen64 seed
  race_ (faultInjectorThread threadIds gen) $
    forConcurrently_ initials $ \x -> do
      tid <- myThreadId
      modifyMVarMasked_ threadIds (pure . (tid :))
      f x
  where
    faultProbabilityClamped = max (min 1 faultProbability) 0
    faultInjectorThread threadIds gen = do
      threadDelay 10_000
      let (num, newGen) = uniformR @Double (0.0, 1.0) gen
      if num > faultProbabilityClamped
        then faultInjectorThread threadIds newGen
        else do
          threads <- readMVar threadIds
          let (shuffled, newGen') = uniformShuffleList threads newGen
          case shuffled of
            [] -> pure ()
            (tid : _) -> throwTo tid TestFault
          faultInjectorThread threadIds newGen'

raftTrace ::
  (Typeable entry, Typeable result, Typeable node, Typeable state) =>
  Trace (SimResult a) SimEvent -> [RaftTrace entry result node state]
raftTrace =
  selectTraceEvents
    ( \_ ev -> case ev of
        EventLog dyn -> fromDynamic dyn
        _ -> Nothing -- internal io-sim event
    )

leaderElected :: Predicate (RaftTrace entry result node state) (Term, node)
leaderElected = predicate $ \case
  (LeaderElected t n) -> Just (t, n)
  _ -> Nothing

votedFor :: Predicate (RaftTrace entry result node state) (Term, node, Term, node)
votedFor = predicate $ \case
  VotedFor voterTerm voterNode candidateTerm candidateNode -> Just (voterTerm, voterNode, candidateTerm, candidateNode)
  _ -> Nothing

commandReceived :: Predicate (RaftTrace entry result node state) (Term, node, Command entry)
commandReceived = predicate $ \case
  (CommandReceived t n command@(Command {})) -> Just (t, n, command)
  _ -> Nothing

commandResponded :: Predicate (RaftTrace entry result node state) (Term, node, CommandResponse node result)
commandResponded = predicate $ \case
  (CommandResultResponded t n response) -> Just (t, n, response)
  _ -> Nothing

clusterMembershipChangeInitiated :: Predicate (RaftTrace entry result node state) (Term, node)
clusterMembershipChangeInitiated = predicate $ \case
  MembershipChangeInitiated t n -> pure (t, n)
  _ -> Nothing

clusterMembershipChangeCompleted :: Predicate (RaftTrace entry result node state) (Term, node)
clusterMembershipChangeCompleted = predicate $ \case
  MembershipChangeCompleted t n -> pure (t, n)
  _ -> Nothing

rpcReceived :: Predicate (RaftTrace entry result node state) (Term, node, RPC node entry state)
rpcReceived = predicate $ \case
  (EventReceived t n (EventRPC rpc)) -> Just (t, n, rpc)
  _ -> Nothing

rpcResultReceived :: Predicate (RaftTrace entry result node state) (Term, node, RPCResult node result)
rpcResultReceived = predicate $ \case
  (EventReceived t n (EventRPCResult resp)) -> Just (t, n, resp)
  _ -> Nothing

commitIndexIncreased :: Predicate (RaftTrace entry result node state) (Term, node, LogIndex)
commitIndexIncreased = predicate $ \case
  (CommitIndexIncreasedTo t n logIndex) -> Just (t, n, logIndex)
  _ -> Nothing

logEntryApplied :: Predicate (RaftTrace entry result node state) (Term, node, entry)
logEntryApplied = predicate $ \case
  (LogEntryApplied t n entry) -> Just (t, n, entry)
  _ -> Nothing
