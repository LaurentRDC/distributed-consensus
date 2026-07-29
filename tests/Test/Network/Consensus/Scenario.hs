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
import Control.Monad.IOSim (SimEvent, SimEventType (EventLog), Trace, selectTraceEvents')
import Control.Monitor
import Data.Dynamic (Typeable, fromDynamic)
import qualified Data.Text as Text
import Data.Word (Word64)
import qualified Debug.Trace as Debug
import Network.Consensus.Raft (Command, CommandResponse, LogIndex, RPC (..), RPCResult, RaftTrace (..), Term)
import System.Random.Stateful (mkStdGen64, uniformR, uniformShuffleList)
import Test.Tasty.QuickCheck

type Scenario entry result node =
  Monitor (RaftTrace entry result node) ()

-- | Check a scenario over a 'Trace'.
checkScenario ::
  ( Show entry,
    Show result,
    Show node,
    Typeable entry,
    Typeable result,
    Typeable node
  ) =>
  Scenario entry result node ->
  Trace a SimEvent ->
  Property
checkScenario scenario trace' =
  let evs = raftTrace trace'
   in case runMonitor scenario evs of
        Left errs -> counterexample (Text.unpack $ ppReasonsWithTrace Text.show 3 (zip [0 ..] evs) errs) False
        Right _ -> True === True

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
    faultProbabilityClamped = Debug.traceShowId $ max (min 1 faultProbability) 0
    faultInjectorThread threadIds gen = do
      threadDelay 10_000
      let (num, newGen) = uniformR @Double (0.0, 1.0) gen
      Debug.traceShowM num
      if num > faultProbabilityClamped
        then faultInjectorThread threadIds newGen
        else do
          threads <- readMVar threadIds
          Debug.traceShowM threads
          let (shuffled, newGen') = uniformShuffleList threads newGen
          case shuffled of
            [] -> pure ()
            (tid : _) -> throwTo tid TestFault
          faultInjectorThread threadIds newGen'

raftTrace ::
  (Typeable entry, Typeable result, Typeable node) =>
  Trace a SimEvent -> [RaftTrace entry result node]
raftTrace =
  selectTraceEvents'
    ( \_ ev -> case ev of
        EventLog dyn -> fromDynamic dyn
        _ -> Nothing -- internal io-sim event
    )

leaderElected :: Predicate (RaftTrace entry result node) (Term, node)
leaderElected = predicate $ \case
  (LeaderElected t n) -> Just (t, n)
  _ -> Nothing

votedFor :: Predicate (RaftTrace entry result node) (Term, node, Term, node)
votedFor = predicate $ \case
  VotedFor voterTerm voterNode candidateTerm candidateNode -> Just (voterTerm, voterNode, candidateTerm, candidateNode)
  _ -> Nothing

commandReceived :: Predicate (RaftTrace entry result node) (Term, node, Command entry)
commandReceived = predicate $ \case
  (CommandReceived t n command) -> Just (t, n, command)
  _ -> Nothing

commandResponded :: Predicate (RaftTrace entry result node) (Term, node, CommandResponse node result)
commandResponded = predicate $ \case
  (CommandResultResponded t n response) -> Just (t, n, response)
  _ -> Nothing

rpcReceived :: Predicate (RaftTrace entry result node) (Term, node, RPC node entry)
rpcReceived = predicate $ \case
  (RPCReceived t n rpc) -> Just (t, n, rpc)
  _ -> Nothing

rpcResultReceived :: Predicate (RaftTrace entry result node) (Term, node, RPCResult node result)
rpcResultReceived = predicate $ \case
  (RPCResultReceived t n resp) -> Just (t, n, resp)
  _ -> Nothing

commitIndexIncreased :: Predicate (RaftTrace entry result node) (Term, node, LogIndex)
commitIndexIncreased = predicate $ \case
  (CommitIndexIncreasedTo t n logIndex) -> Just (t, n, logIndex)
  _ -> Nothing

logEntryApplied :: Predicate (RaftTrace entry result node) (Term, node, entry)
logEntryApplied = predicate $ \case
  (LogEntryApplied t n entry) -> Just (t, n, entry)
  _ -> Nothing
