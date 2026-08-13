{-# LANGUAGE ExplicitForAll #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

module Test.Network.Consensus.Raft.Properties
  ( allProperties,

    -- * Individual properties
    singleLeaderPerTermProperty,
    singleVoteCastPerTermProperty,
    monotonicallyIncreasingTermProperty,
    allAcceptedCommandReceiveResponseProperty,
    allAcceptedClusterMembershipRequestsResultInNewClusterConfiguration,
    indexRelationshipProperty,
    monotonicallyIncreasingLastAppliedIndexProperty,
    monotonicallyIncreasingCommitIndexProperty,
  )
where

import Control.Monad (unless, void, when)
import Control.Monitor
import qualified Data.Map.Strict as Map
import qualified Data.Set as Set
import qualified Data.Text as Text
import Network.Consensus.Raft
  ( Command (..),
    CommandResponse (..),
    LogIndex,
    RaftTrace (..),
  )
import Test.Network.Consensus.Scenario (Scenario, clusterMembershipChangeCompleted, clusterMembershipChangeInitiated, commandReceived, commandResponded, leaderElected, votedFor)

-- It's still not clear to me why, but without the explicit forall here,
-- RaftTrace events don't get picked up
allProperties :: forall entry result node state. (Ord node) => Scenario entry result node state
allProperties =
  void $
    allOf
      [ singleLeaderPerTermProperty,
        singleVoteCastPerTermProperty,
        monotonicallyIncreasingTermProperty,
        allAcceptedCommandReceiveResponseProperty,
        allAcceptedClusterMembershipRequestsResultInNewClusterConfiguration,
        indexRelationshipProperty,
        monotonicallyIncreasingLastAppliedIndexProperty,
        monotonicallyIncreasingCommitIndexProperty
      ]

-- | Ensure that in each term where a leader is elected, no other leader
-- is elected
singleLeaderPerTermProperty :: Scenario entry result node state
singleLeaderPerTermProperty = go <?> "Another leader elected for the same term"
  where
    anotherLeaderIn term = predicate $ \case
      LeaderElected t _ | t == term -> Just ()
      _ -> Nothing

    -- Whenever a leader is elected, there should not be another
    -- leader during this term.
    --
    -- In order to allow the test to span multiple terms, we need to recursively
    -- apply the expectation using 'both'
    go = void $ whenever leaderElected $ \(term, _) ->
      both go (never (anotherLeaderIn term))

-- | Ensure that a node casts at most one vote per term
singleVoteCastPerTermProperty :: (Ord node) => Scenario entry result node state
singleVoteCastPerTermProperty = go mempty <?> "More than one vote cast per term"
  where
    go votes =
      void
        ( whenever
            votedFor
            $ \(voterTerm, voterNode, _, _) -> do
              when (Set.member (voterTerm, voterNode) votes) $ fail "Node cast more than one vote in term"

              go (Set.insert (voterTerm, voterNode) votes)
        )

-- | Ensure that each node witnesses terms that increase monotonically
monotonicallyIncreasingTermProperty :: (Ord node) => Scenario entry result node state
monotonicallyIncreasingTermProperty = go mempty <?> "Term not increased monotonically"
  where
    go latestKnownTerms = void $ do
      whenever (predicate roleTerm) $ \(node, newTerm) -> do
        case Map.lookup node latestKnownTerms of
          Nothing -> pure ()
          Just latestTerm ->
            assert
              "Expecting terms to increase monotonically"
              (latestTerm <= newTerm)
        go (Map.insert node newTerm latestKnownTerms)

    roleTerm (LeaderElected t n) = Just (n, t)
    roleTerm (BecameCandidate t n) = Just (n, t)
    roleTerm (BecameFollower t n) = Just (n, t)
    roleTerm _ = Nothing

-- | Ensure that each node's last applied index increases monotonically.
monotonicallyIncreasingLastAppliedIndexProperty :: (Ord node) => Scenario entry result node state
monotonicallyIncreasingLastAppliedIndexProperty = go mempty <?> "Last applied index not increasing monotonically"
  where
    go acc =
      step
        ()
        ( \case
            LastAppliedIndexIncreasedTo _term node lastApplied -> do
              case Map.lookup node acc of
                Nothing -> pure ()
                Just prevLastApplied ->
                  assert
                    "Expecting last applied index to increase monotonically"
                    (prevLastApplied <= lastApplied)

              go (Map.insert node lastApplied acc)
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
            CommitIndexIncreasedTo _term node commitIndex -> do
              case Map.lookup node acc of
                Nothing -> pure ()
                Just prevCommitIndex ->
                  assert
                    "Expecting commit index to increase monotonically"
                    (prevCommitIndex <= commitIndex)

              go (Map.insert node commitIndex acc)
            _ -> go acc
        )

-- | Ensure that every command that was acknowledged by the leader
-- receives a response with the same request ID.
allAcceptedCommandReceiveResponseProperty :: Scenario entry result node state
allAcceptedCommandReceiveResponseProperty = go
  where
    go = void $ whenever commandReceived $ \(_, _, Command _entry reqId) ->
      let thisCommandResponded =
            commandResponded >>= \(_, _, MkCommandResponse _result' reqId') ->
              predicate $ \_ -> unless (reqId == reqId') Nothing
       in eventually thisCommandResponded <?> "Never received a response for " <> Text.show reqId

-- | Ensure that every cluster membership change that's accepted by a leader
-- is seen through by the same leader
allAcceptedClusterMembershipRequestsResultInNewClusterConfiguration :: (Eq node) => Scenario entry result node state
allAcceptedClusterMembershipRequestsResultInNewClusterConfiguration =
  go <?> "An accepted cluster membership request did not result in new configuration"
  where
    go = void $ whenever clusterMembershipChangeInitiated $ \(term, leader) ->
      eventually
        ( clusterMembershipChangeCompleted >>= \(term', leader') ->
            predicate $ \_ -> unless (term == term' && leader == leader') Nothing
        )
        <?> "Cluster membership change never completed"

data NodeIndexes
  = MkNodeIndexes
      -- | Last applied index
      !LogIndex
      -- | Commit index
      !LogIndex
      -- | Log length
      !Int

-- | Ensures that the following relationship holds for each node individually:
--
--    appliex index  <=  commit index  <=  log length
indexRelationshipProperty :: (Ord node) => Scenario entry result node state
indexRelationshipProperty = go mempty <?> "index relationship did not hold"
  where
    checkState node acc =
      case Map.lookup node acc of
        Nothing -> assert "Incomplete state" False
        Just (MkNodeIndexes lastApplied committed logLength) -> do
          assert "last_applied_index <= commit_index violated" (lastApplied <= committed)
          assert "commit_index <= log_length violated" (committed <= fromIntegral logLength)
    go acc =
      step
        ()
        ( \case
            LastAppliedIndexIncreasedTo _term node lastApplied -> do
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
            CommitIndexIncreasedTo _term node commitIndex -> do
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
            LogEntryAppended _tern node _ -> do
              let newAcc =
                    Map.alter
                      ( \case
                          Nothing -> Just (MkNodeIndexes 0 0 1)
                          Just (MkNodeIndexes x y z) -> Just (MkNodeIndexes x y (succ z))
                      )
                      node
                      acc
              checkState node newAcc
              go newAcc
            _ -> go acc
        )
