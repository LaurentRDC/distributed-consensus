{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Distributed.Consensus.Raft.Domain
  ( Term,
    InternalRequestId,
    RequestId,
    mkRequestId,
    requestTerm,
    clientRequestId,
    Role (..),
    ClusterConfiguration (..),
    allNodes,
    hasQuorum,
    LogIndex,

    -- * Snapshots
    Snapshot (..),
    SnapshotMetadata (..),

    -- * State machine
    StateMachineStep (..),
    runStateMachineStep,
  )
where

import Data.Binary (Binary)
import Data.Int (Int64)
import Data.Set (Set)
import qualified Data.Set as Set
import Data.Word (Word64)
import Distributed.Consensus.Raft.Domain.Client (ClientRequestId)
import GHC.Generics (Generic)

-- | Election term
newtype Term = Term Int64
  deriving stock (Generic, Eq, Ord, Show, Read)
  deriving newtype (Real, Binary, Enum, Num, Integral)

newtype InternalRequestId = InternalRequestId Word64
  deriving stock (Generic, Eq, Ord, Show)
  deriving newtype (Real, Binary, Enum, Num, Integral)

-- | A 'RequestId' identifies a request from a client, uniquely across the
-- whole history of a cluster.
--
-- In order to support multiple concurrent clients, we mix the client-provided
-- ID with a monotonic internal ID. However, the monotonic internal ID is lost
-- on crash, so we include the term as well.
data RequestId = RequestId
  { requestTerm :: !Term,
    internalRequestId :: !InternalRequestId,
    clientRequestId :: !ClientRequestId
  }
  deriving stock (Generic, Eq, Ord, Show)

instance Binary RequestId

mkRequestId :: Term -> InternalRequestId -> ClientRequestId -> RequestId
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

-- | 'LogIndex' is an absolute position within the 'Log'.
--
-- Note that the 'Log' may not have the elements associated with a given
-- 'LogIndex' due to compaction.
newtype LogIndex = LogIndex Int64
  deriving stock (Generic, Eq, Ord, Show)
  deriving newtype (Real, Enum, Num, Binary, Integral)

data SnapshotMetadata = SnapshotMetadata
  { smLastIncludedIndex :: !LogIndex,
    smLastIncludedTerm :: !Term
  }
  deriving (Eq, Ord, Show, Generic)

instance Binary SnapshotMetadata

data Snapshot node state = Snapshot
  { sMetadata :: !SnapshotMetadata,
    sData :: !state,
    sCluster :: !(ClusterConfiguration node)
  }
  deriving (Eq, Ord, Show, Generic)

instance (Binary state, Binary node) => Binary (Snapshot node state)

-- | A state machine entry results in a step which can either read the state ('Read')
-- or mutate the state ('Write').
--
-- The distinction is important because reading from the state can be optimized
-- differently than a mutation.
--
-- >>> data CounterOp = ReadCounter | Increment | Decrement
-- >>> :{ let stateMachine = \op -> case op of
--    ReadCounter -> Read id
--    Increment -> Write $ \st -> (succ st, succ st)
--    Decrement -> Write $ \st -> (pred st, pred st)
-- :}
data StateMachineStep state result
  = Read (state -> result)
  | Transition (state -> (state, result))

runStateMachineStep :: StateMachineStep state result -> state -> (state, result)
runStateMachineStep (Read runRead) st = (st, runRead st)
runStateMachineStep (Transition runTrans) st = runTrans st
