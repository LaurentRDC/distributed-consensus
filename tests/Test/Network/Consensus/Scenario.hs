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

leaderElected :: RaftTrace entry result node -> Maybe (Term, node)
leaderElected (LeaderElected t n) = Just (t, n)
leaderElected _ = Nothing

commandReceived :: RaftTrace entry result node -> Maybe (Term, node, Command node entry)
commandReceived (CommandReceived t n command) = Just (t, n, command)
commandReceived _ = Nothing

rpcReceived :: RaftTrace entry result node -> Maybe (Term, node, RPC node entry)
rpcReceived (RPCReceived t n rpc) = Just (t, n, rpc)
rpcReceived _ = Nothing

rpcResultReceived :: RaftTrace entry result node -> Maybe (Term, node, RPCResult node result)
rpcResultReceived (RPCResultReceived t n resp) = Just (t, n, resp)
rpcResultReceived _ = Nothing

commitIndexIncreased :: RaftTrace entry result node -> Maybe (Term, node, LogIndex)
commitIndexIncreased (CommitIndexIncreasedTo t n logIndex) = Just (t, n, logIndex)
commitIndexIncreased _ = Nothing

logEntryApplied :: RaftTrace entry result node -> Maybe (Term, node, entry)
logEntryApplied (LogEntryApplied t n entry) = Just (t, n, entry)
logEntryApplied _ = Nothing
