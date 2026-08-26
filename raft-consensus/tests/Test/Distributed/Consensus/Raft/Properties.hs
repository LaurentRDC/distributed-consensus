{-# LANGUAGE ExplicitForAll #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

module Test.Distributed.Consensus.Raft.Properties
  ( FaultInjection (..),
    allProperties,

    -- * Individual properties
    singleLeaderPerTermProperty,
    singleVoteCastPerTermProperty,
    monotonicallyIncreasingTermProperty,
    allAcceptedCommandReceiveResponseProperty,
    allAcceptedClusterMembershipRequestsResultInNewClusterConfiguration,
    indexRelationshipProperty,
    monotonicallyIncreasingLastAppliedIndexProperty,
    monotonicallyIncreasingCommitIndexProperty,
    crashRecoveryProperty,
  )
where

import Control.Monad (unless, void)
import Control.Monitor
  ( allOf,
    assert,
    eventually,
    predicate,
    step,
    whenever,
    (<?>),
  )
import qualified Data.Map.Strict as Map
import qualified Data.Set as Set
import qualified Data.Text as Text
import Distributed.Consensus.Raft (CommandResponse (..), EventContext (..), LogIndex, RaftTrace (..))
import Test.Distributed.Consensus.Raft.Scenario
  ( Scenario,
    clusterMembershipChangeCompleted,
    clusterMembershipChangeInitiated,
    commandReceived,
    commandResponded,
    crashed,
    joinClusterCommandReceived,
    leaveClusterCommandReceived,
    membershipSettled,
    stateRestored,
  )

-- | Whether we expect the cluster to always respond, or not.
--
-- In general, with faults, Raft clusters are not expected to be live.
-- However, if the fault injector is turned off, we DO expect an answer!
data FaultInjection
  = NoFaultInjection
  | FaultInjection

-- It's still not clear to me why, but without the explicit forall here,
-- RaftTrace events don't get picked up.
allProperties :: forall entry result node state. (Ord node, Show node) => FaultInjection -> Scenario entry result node state
allProperties faultInjection =
  let common =
        [ singleLeaderPerTermProperty,
          singleVoteCastPerTermProperty,
          monotonicallyIncreasingTermProperty,
          indexRelationshipProperty,
          monotonicallyIncreasingLastAppliedIndexProperty,
          monotonicallyIncreasingCommitIndexProperty,
          crashRecoveryProperty
        ]
      extras = case faultInjection of
        NoFaultInjection ->
          [ allAcceptedCommandReceiveResponseProperty,
            allAcceptedClusterMembershipRequestsResultInNewClusterConfiguration,
            allAdminRequestToJoinComplete,
            allAdminRequestToLeaveComplete
          ]
        FaultInjection -> []
   in void $
        allOf (common <> extras)

-- | Ensure that in each term where a leader is elected, no other leader
-- is elected
singleLeaderPerTermProperty :: Scenario entry result node state
singleLeaderPerTermProperty = go mempty <?> "Another leader elected for the same term"
  where
    go seenTerms =
      step
        ()
        ( \case
            LeaderElected (EventContext term _) -> do
              assert
                "Expecting at most one leader per term"
                (Set.notMember term seenTerms)
              go (Set.insert term seenTerms)
            _ -> go seenTerms
        )

-- | Ensure that a node casts at most one vote per term
--
-- Note that under fault conditions, the same vote for the same term may be cast multiple
-- times. I consider this acceptable; votes are idempotent
singleVoteCastPerTermProperty :: (Ord node, Show node) => Scenario entry result node state
singleVoteCastPerTermProperty = go mempty <?> "More than one vote cast per term"
  where
    go votes =
      step
        ()
        ( \case
            VotedFor (EventContext voterTerm voterNode) _ candidate -> do
              assert
                ("Expecting a node to cast at most one vote per term: " <> Text.show votes)
                ( case Map.lookup (voterTerm, voterNode) votes of
                    Nothing -> True
                    Just candidate' -> candidate == candidate' -- allow multiple identical vote for idempotence
                )
              go (Map.insert (voterTerm, voterNode) candidate votes)
            _ -> go votes
        )

-- | Ensure that each node witnesses terms that increase monotonically
monotonicallyIncreasingTermProperty :: (Ord node) => Scenario entry result node state
monotonicallyIncreasingTermProperty = go mempty <?> "Term not increased monotonically"
  where
    go latestKnownTerms =
      step
        ()
        ( \e -> case roleTerm e of
            Nothing -> go latestKnownTerms
            Just (node, newTerm) -> do
              case Map.lookup node latestKnownTerms of
                Nothing -> pure ()
                Just latestTerm ->
                  assert
                    "Expecting terms to increase monotonically"
                    (latestTerm <= newTerm)
              go (Map.insert node newTerm latestKnownTerms)
        )

    roleTerm (LeaderElected (EventContext t n)) = Just (n, t)
    roleTerm (BecameCandidate (EventContext t n)) = Just (n, t)
    roleTerm (BecameFollower (EventContext t n)) = Just (n, t)
    roleTerm _ = Nothing

-- | Ensure that each node's last applied index increases monotonically.
monotonicallyIncreasingLastAppliedIndexProperty :: (Ord node) => Scenario entry result node state
monotonicallyIncreasingLastAppliedIndexProperty = go mempty <?> "Last applied index not increasing monotonically"
  where
    go acc =
      step
        ()
        ( \case
            LastAppliedIndexIncreasedTo (EventContext _ node) lastApplied -> do
              case Map.lookup node acc of
                Nothing -> pure ()
                Just prevLastApplied ->
                  assert
                    "Expecting last applied index to increase monotonically"
                    (prevLastApplied <= lastApplied)

              go (Map.insert node lastApplied acc)
            Crashed node -> go (Map.delete node acc)
            _ -> go acc
        )

-- | Ensure that each node's commit index increases monotonically.
monotonicallyIncreasingCommitIndexProperty :: (Ord node) => Scenario entry result node state
monotonicallyIncreasingCommitIndexProperty = go mempty <?> "Commit index not increasing monotonically"
  where
    go acc =
      step
        ()
        ( \case
            CommitIndexIncreasedTo (EventContext _ node) commitIndex -> do
              case Map.lookup node acc of
                Nothing -> pure ()
                Just prevCommitIndex ->
                  assert
                    "Expecting commit index to increase monotonically"
                    (prevCommitIndex <= commitIndex)

              go (Map.insert node commitIndex acc)
            Crashed node -> go (Map.delete node acc)
            _ -> go acc
        )

-- | Ensure that every command that was acknowledged by the leader
-- receives a response with the same request ID.
allAcceptedCommandReceiveResponseProperty :: Scenario entry result node state
allAcceptedCommandReceiveResponseProperty = go
  where
    go = void $ whenever commandReceived $ \(_, reqId, _) ->
      let thisCommandResponded =
            commandResponded >>= \(_, MkCommandResponse _result' reqId') ->
              predicate $ \_ -> unless (reqId == reqId') Nothing
       in eventually thisCommandResponded <?> "Never received a response for " <> Text.show reqId

-- | Ensure that every cluster membership change that's accepted by a leader
-- is seen through by the same leader
allAcceptedClusterMembershipRequestsResultInNewClusterConfiguration :: (Eq node) => Scenario entry result node state
allAcceptedClusterMembershipRequestsResultInNewClusterConfiguration =
  go <?> "An accepted cluster membership request did not result in new configuration"
  where
    go = void $ whenever clusterMembershipChangeInitiated $ \ctx ->
      eventually
        ( clusterMembershipChangeCompleted >>= \ctx' ->
            predicate $ \_ -> unless (ctx == ctx') Nothing
        )

allAdminRequestToJoinComplete :: (Ord node, Show node) => Scenario entry result node state
allAdminRequestToJoinComplete =
  go <?> "A command to join a cluster did not result in new cluster membership"
  where
    -- The authority on whether a node is in the cluster is the
    -- leader that applies the new configuration, not the joining node
    go = void $ whenever joinClusterCommandReceived $ \(EventContext _ nodeJoining) ->
      eventually (membershipSettled (Set.member nodeJoining) nodeJoining)
        <?> "A command to "
        <> Text.show nodeJoining
        <> " to join a cluster did not result in new cluster membership"

allAdminRequestToLeaveComplete :: (Show node, Ord node) => Scenario entry result node state
allAdminRequestToLeaveComplete =
  go
  where
    go = void $ whenever leaveClusterCommandReceived $ \(EventContext _ nodeLeaving) ->
      -- This is quite subtle. Basically, the cluster can replicate, commit, and apply a cluster
      -- configuration change without the node leaving the cluster ever knowing. Consider a cluster
      -- of three nodes [A, B, C], where C is leaving; join configurations [A, B] and [A, B, C] have
      -- quorum with just A and B acknowledging and applying the configuration state.
      --
      -- Therefore, the true mark of whether a node has left a cluster is not from the point-of-view
      -- of that node, but from the point-of-view of the leader, that emits a
      -- 'MembershipChangeApplied'
      eventually (membershipSettled (Set.notMember nodeLeaving) nodeLeaving)
        <?> "A command to "
        <> Text.show nodeLeaving
        <> " to leave a cluster did not result in new cluster membership"

data NodeIndexes
  = MkNodeIndexes
      -- | Last applied index
      !LogIndex
      -- | Commit index
      !LogIndex
      -- | Last log index
      !LogIndex

-- | Ensures that the following relationship holds for each node individually:
--
--    appliex index  <=  commit index  <=  log length
indexRelationshipProperty :: (Ord node) => Scenario entry result node state
indexRelationshipProperty = go mempty <?> "index relationship did not hold"
  where
    checkState node acc =
      case Map.lookup node acc of
        Nothing -> assert "Incomplete state" False
        Just (MkNodeIndexes lastApplied committed lastIndex) -> do
          assert "last_applied_index <= commit_index violated" (lastApplied <= committed)
          assert "commit_index <= last_index violated" (committed <= lastIndex)
    go acc =
      step
        ()
        ( \case
            LastAppliedIndexIncreasedTo (EventContext _ node) lastApplied -> do
              let newAcc =
                    Map.alter
                      ( \case
                          Nothing -> Just (MkNodeIndexes lastApplied 0 0)
                          Just (MkNodeIndexes _ y z) -> Just (MkNodeIndexes lastApplied y z)
                      )
                      node
                      acc
              checkState node newAcc
              go newAcc
            CommitIndexIncreasedTo (EventContext _ node) commitIndex -> do
              let newAcc =
                    Map.alter
                      ( \case
                          Nothing -> Just (MkNodeIndexes 0 commitIndex 0)
                          Just (MkNodeIndexes x _ z) -> Just (MkNodeIndexes x commitIndex z)
                      )
                      node
                      acc
              checkState node newAcc
              go newAcc
            LastLogIndexChangedTo (EventContext _ node) lastLogIndex -> do
              let newAcc =
                    Map.alter
                      ( \case
                          Nothing -> Just (MkNodeIndexes 0 0 lastLogIndex)
                          Just (MkNodeIndexes x y _) -> Just (MkNodeIndexes x y lastLogIndex)
                      )
                      node
                      acc
              checkState node newAcc
              go newAcc
            Crashed node -> go (Map.delete node acc)
            _ -> go acc
        )

crashRecoveryProperty :: (Eq node, Show node) => Scenario entry result node state
crashRecoveryProperty = go
  where
    go = void $ whenever crashed $ \node ->
      eventually
        ( stateRestored >>= \(EventContext _ node', _, _) -> predicate $ \_ ->
            unless (node' == node) Nothing
        )
        <?> ("Node " <> Text.show node <> " did not restore state after crash")
