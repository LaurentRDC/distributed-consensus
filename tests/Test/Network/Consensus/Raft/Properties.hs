{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

module Test.Network.Consensus.Raft.Properties
  ( allProperties,

    -- * Individual properties
    singleLeaderPerTermProperty,
    singleVoteCastPerTermProperty,
    monotonicallyIncreasingTermProperty,
    allAcceptedCommandReceiveResponseProperty,
  )
where

import Control.Monad (unless, void, when)
import Control.Monitor
import qualified Data.Map.Strict as Map
import qualified Data.Set as Set
import Network.Consensus.Raft
  ( Command (..),
    CommandResponse (..),
    RaftTrace (..),
  )
import Test.Network.Consensus.Scenario (Scenario, commandReceived, commandResponded, leaderElected, votedFor)

allProperties :: (Ord node) => Scenario entry result node
allProperties =
  void $
    allOf
      [ singleLeaderPerTermProperty,
        singleVoteCastPerTermProperty,
        monotonicallyIncreasingTermProperty,
        allAcceptedCommandReceiveResponseProperty
      ]

-- | Ensure that in each term where a leader is elected, no other leader
-- is elected
singleLeaderPerTermProperty :: Scenario entry result node
singleLeaderPerTermProperty = go
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
        <?> "Another leader elected for the same term"

-- | Ensure that a node casts at most one vote per term
singleVoteCastPerTermProperty :: (Ord node) => Scenario entry result node
singleVoteCastPerTermProperty = go mempty
  where
    go votes = void $ whenever votedFor $ \(voterTerm, voterNode, _, _) -> do
      when (Set.member (voterTerm, voterNode) votes) $ fail "Node cast more than one vote in term"

      go (Set.insert (voterTerm, voterNode) votes)

-- | Ensure that each node witnesses terms that increase monotonically
monotonicallyIncreasingTermProperty :: (Ord node) => Scenario entry result node
monotonicallyIncreasingTermProperty = go mempty
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

-- | Ensure that every command that was acknowledged by the leader
-- receives a response with the same request ID.
allAcceptedCommandReceiveResponseProperty :: Scenario entry result node
allAcceptedCommandReceiveResponseProperty = go
  where
    go = void $ whenever commandReceived $ \(_, _, MkCommand _node _entry reqId) ->
      let thisCommandResponded =
            commandResponded >>= \(_, _, MkCommandResponse _node' _result' reqId') ->
              predicate $ \_ -> unless (reqId == reqId') Nothing
       in eventually thisCommandResponded
