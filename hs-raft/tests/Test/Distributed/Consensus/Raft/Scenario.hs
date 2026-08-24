{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Test.Distributed.Consensus.Raft.Scenario
  ( Scenario,
    checkScenario,
    module Control.Monitor,

    -- * Helpers
    stateRestored,
    leaderElected,
    votedFor,
    commandReceived,
    commandResponded,
    joinClusterCommandReceived,
    leaveClusterCommandReceived,
    joinedCluster,
    clusterMembershipChangeInitiated,
    clusterMembershipChangeCompleted,
    clusterMembershipChangeApplied,
    commitIndexIncreased,
    logEntryApplied,
    rpcReceived,
    rpcResultReceived,
    crashed,
    membershipSettled,
  )
where

import Control.Monad.IOSim (SimEvent, SimEventType (EventLog), SimResult, Trace, selectTraceEvents)
import Control.Monitor (Monitor, Predicate, ppReasonsWithTrace, predicate, runMonitor)
import Data.Dynamic (Typeable, fromDynamic)
import Data.Set (Set)
import qualified Data.Text as Text
import Distributed.Consensus.Raft
  ( ClusterConfiguration (..),
    CommandResponse,
    Event (..),
    EventContext,
    Log,
    LogEntry,
    LogIndex,
    RPC (..),
    RPCResult,
    RaftTrace (..),
    Request (..),
    RequestId,
    Term,
  )
import Distributed.Consensus.Raft.Admin (AdminCommand (..))
import Test.Tasty.QuickCheck (Property, counterexample)

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

raftTrace ::
  (Typeable entry, Typeable result, Typeable node, Typeable state) =>
  Trace (SimResult a) SimEvent -> [RaftTrace entry result node state]
raftTrace =
  selectTraceEvents
    ( \_ ev -> case ev of
        EventLog dyn -> fromDynamic dyn
        _ -> Nothing -- internal io-sim event
    )

stateRestored :: Predicate (RaftTrace entry result node state) (EventContext node, Maybe node, Log node (LogEntry node entry))
stateRestored = predicate $ \case
  StateRestored ctx mVote logEntries -> Just (ctx, mVote, logEntries)
  _ -> Nothing

leaderElected :: Predicate (RaftTrace entry result node state) (EventContext node)
leaderElected = predicate $ \case
  (LeaderElected ctx) -> Just ctx
  _ -> Nothing

votedFor :: Predicate (RaftTrace entry result node state) (EventContext node, Term, node)
votedFor = predicate $ \case
  VotedFor ctx candidateTerm candidateNode -> Just (ctx, candidateTerm, candidateNode)
  _ -> Nothing

commandReceived :: Predicate (RaftTrace entry result node state) (EventContext node, RequestId, entry)
commandReceived = predicate $ \case
  (CommandReceived ctx reqId entry) -> Just (ctx, reqId, entry)
  _ -> Nothing

commandResponded :: Predicate (RaftTrace entry result node state) (EventContext node, CommandResponse node result)
commandResponded = predicate $ \case
  (CommandResultResponded ctx response) -> Just (ctx, response)
  _ -> Nothing

joinClusterCommandReceived :: Predicate (RaftTrace entry result node state) (EventContext node)
joinClusterCommandReceived = predicate $ \case
  (AdminRequestReceived ctx (MkRequest _ _ (JoinCluster _))) -> Just ctx
  _ -> Nothing

leaveClusterCommandReceived :: Predicate (RaftTrace entry result node state) (EventContext node)
leaveClusterCommandReceived = predicate $ \case
  (AdminRequestReceived ctx (MkRequest _ _ LeaveCluster)) -> Just ctx
  _ -> Nothing

joinedCluster :: Predicate (RaftTrace entry result node state) (EventContext node)
joinedCluster = predicate $ \case
  JoinedCluster ctx -> Just ctx
  _ -> Nothing

clusterMembershipChangeInitiated :: Predicate (RaftTrace entry result node state) (EventContext node)
clusterMembershipChangeInitiated = predicate $ \case
  MembershipChangeInitiated ctx -> pure ctx
  _ -> Nothing

clusterMembershipChangeCompleted :: Predicate (RaftTrace entry result node state) (EventContext node)
clusterMembershipChangeCompleted = predicate $ \case
  MembershipChangeCompleted ctx -> pure ctx
  _ -> Nothing

clusterMembershipChangeApplied :: Predicate (RaftTrace entry result node state) (EventContext node, ClusterConfiguration node)
clusterMembershipChangeApplied = predicate $ \case
  MembershipChangeApplied ctx clusterConf -> pure (ctx, clusterConf)
  _ -> Nothing

rpcReceived :: Predicate (RaftTrace entry result node state) (EventContext node, RPC node entry state)
rpcReceived = predicate $ \case
  (EventReceived ctx (EventRPC rpc)) -> Just (ctx, rpc)
  _ -> Nothing

rpcResultReceived :: Predicate (RaftTrace entry result node state) (EventContext node, RPCResult node result)
rpcResultReceived = predicate $ \case
  (EventReceived ctx (EventRPCResult resp)) -> Just (ctx, resp)
  _ -> Nothing

commitIndexIncreased :: Predicate (RaftTrace entry result node state) (EventContext node, LogIndex)
commitIndexIncreased = predicate $ \case
  (CommitIndexIncreasedTo ctx logIndex) -> Just (ctx, logIndex)
  _ -> Nothing

logEntryApplied :: Predicate (RaftTrace entry result node state) (EventContext node, entry)
logEntryApplied = predicate $ \case
  (LogEntryApplied ctx entry) -> Just (ctx, entry)
  _ -> Nothing

crashed :: Predicate (RaftTrace entry result node state) node
crashed = predicate $ \case
  Crashed node -> Just node
  _ -> Nothing

membershipSettled :: (Eq node) => (Set node -> Bool) -> node -> Predicate (RaftTrace entry result node state) ()
membershipSettled wanted node = predicate $ \case
  MembershipChangeApplied _ (Joint _ _) -> Just ()
  MembershipChangeApplied _ (Simple cluster) | wanted cluster -> Just ()
  MembershipChangeAlreadySettled _ node' | node' == node -> Just ()
  _ -> Nothing
