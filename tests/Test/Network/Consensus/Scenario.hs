{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Test.Network.Consensus.Scenario
  ( Scenario,
    checkScenario,
    module Control.Monitor,

    -- * Helpers
    leaderElected,
    commandReceived,
    commitIndexIncreased,
    logEntryApplied,
    rpcReceived,
    rpcResultReceived,
  )
where

import Control.Monad.IOSim (SimEvent, SimEventType (EventLog), Trace, selectTraceEvents')
import Control.Monitor
import Data.Dynamic (Typeable, fromDynamic)
import qualified Data.Text as Text
import Network.Consensus.Raft (Command, LogIndex, RPC (..), RPCResult, RaftTrace (..), Term)
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
   in counterexample ("Failed with trace: " ++ show evs) $ case runMonitor scenario evs of
        Left errs -> counterexample (Text.unpack $ ppReasonsWithTrace Text.show 3 (zip [0 ..] evs) errs) False
        Right _ -> True === True

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

commandReceived :: Predicate (RaftTrace entry result node) (Term, node, Command node entry)
commandReceived = predicate $ \case
  (CommandReceived t n command) -> Just (t, n, command)
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
