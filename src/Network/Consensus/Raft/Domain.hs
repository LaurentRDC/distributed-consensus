{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Network.Consensus.Raft.Domain
  ( Term,
    RequestId,
    Role (..),
    ClusterConfiguration (..),
    allNodes,
    hasQuorum,
  )
where

import Data.Int (Int64)
import Data.Set (Set)
import qualified Data.Set as Set
import GHC.Generics (Generic)

-- | Election term
newtype Term = Term Int64
  deriving stock (Generic, Eq, Ord, Show)
  deriving newtype (Real, Enum, Num, Integral)

-- | A 'RequestId' dentifies a request from a client,
-- uniquely for each leader.
newtype RequestId = RequestId Int64
  deriving stock (Generic, Eq, Ord, Show)
  deriving newtype (Real, Enum, Num, Integral)

data Role
  = Leader
  | Follower
  | Candidate
  deriving (Eq, Show, Ord, Enum, Bounded)

-- | Represents the membership of a cluster.
--
-- In normal operations, a cluster is composed of a particular set of nodes.
-- During membership changes, the cluster is in two overlapping states;
-- one state with all of the previous members, and another state with a different, possibly
-- overlapping, set of members.
data ClusterConfiguration node
  = Simple (Set node)
  | Joint (Set node) (Set node)
  deriving (Eq, Ord, Show)

allNodes :: (Ord node) => ClusterConfiguration node -> Set node
allNodes (Simple nodes) = nodes
allNodes (Joint nodes1 nodes2) = nodes1 `Set.union` nodes2

-- | Whether a set of nodes has a quorum based on current cluster configuration.
--
-- In the case of a joint cluster configuration, a quorum is achieved if
-- a quorum is achieves within both configurations
--
-- >>> :set -XOverloadedLists
-- >>> conf = Joint ['A', 'B', 'C'] ['A', 'C', 'D']
-- >>> hasQuorum conf ['A']
-- False
-- >>> hasQuorum conf ['A', 'B']
-- False
-- >>> hasQuorum conf ['A', 'C']
-- True
-- >>> hasQuorum (Simple ['A']) ['A']
-- True
hasQuorum :: (Ord node) => ClusterConfiguration node -> Set node -> Bool
hasQuorum (Simple members) votes = Set.size (Set.intersection members votes) > Set.size members `div` 2
hasQuorum (Joint members1 members2) votes =
  hasQuorum (Simple members1) votes
    && hasQuorum (Simple members2) votes
