{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Network.Consensus.Raft.Domain
  ( Term,
    InternalRequestId,
    RequestId,
    mkRequestId,
    clientRequestId,
    Role (..),
    ClusterConfiguration (..),
    allNodes,
    hasQuorum,
  )
where

import Data.Binary (Binary)
import Data.Int (Int64)
import Data.Set (Set)
import qualified Data.Set as Set
import Data.Word (Word64)
import GHC.Generics (Generic)
import Network.Consensus.Raft.Client (ClientRequestId)

-- | Election term
newtype Term = Term Int64
  deriving stock (Generic, Eq, Ord, Show)
  deriving newtype (Real, Binary, Enum, Num, Integral)

newtype InternalRequestId = InternalRequestId Word64
  deriving stock (Generic, Eq, Ord, Show)
  deriving newtype (Real, Binary, Enum, Num, Integral)

-- | A 'RequestId' dentifies a request from a client,
-- uniquely for each leader.
--
-- In order to support multiple concurrent clients,
-- we mix the client-provided ID with a monotonic
-- internal ID
data RequestId = RequestId
  { internalRequestId :: !InternalRequestId,
    clientRequestId :: !ClientRequestId
  }
  deriving stock (Generic, Eq, Ord, Show)

instance Binary RequestId

mkRequestId :: InternalRequestId -> ClientRequestId -> RequestId
mkRequestId = RequestId

data Role
  = Leader
  | Follower
  | Candidate
  | -- | Special states for nodes that aren't members of a cluster. This
    -- doesn't let them participate in elections and change their terms
    NonMember
  deriving (Eq, Show, Ord, Enum, Generic, Bounded)

instance Binary Role

-- | Represents the membership of a cluster.
--
-- In normal operations, a cluster is composed of a particular set of nodes.
-- During membership changes, the cluster is in two overlapping states;
-- one state with all of the previous members, and another state with a different, possibly
-- overlapping, set of members.
data ClusterConfiguration node
  = Simple (Set node)
  | Joint (Set node) (Set node)
  deriving (Eq, Ord, Generic, Show)

instance (Binary node) => Binary (ClusterConfiguration node)

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
