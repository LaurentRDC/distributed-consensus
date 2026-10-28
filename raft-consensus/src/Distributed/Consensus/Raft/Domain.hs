{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Distributed.Consensus.Raft.Domain
  ( Term,
    Role (..),
    ClusterConfiguration (..),
    allNodes,
    hasQuorum,
    LogIndex,

    -- * Sessions and sequences
    SessionId,
    sessionId,
    SequenceNum,

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
import GHC.Generics (Generic)
import System.Random (Uniform)

-- | Election term
newtype Term = Term Int64
  deriving stock (Generic, Eq, Ord, Show, Read)
  deriving newtype (Real, Binary, Enum, Num, Integral)

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

-- | Uniquely identifies a session between a cluster and a client or admin, globally. This
-- value should be generated at random.
data SessionId = MkSessionId Word64 Word64
  deriving (Eq, Show, Ord, Generic)

instance Binary SessionId

instance Uniform SessionId

sessionId :: Word64 -> Word64 -> SessionId
sessionId = MkSessionId

-- | Incrementing sequence number for every client or admin request
-- in the same session.
newtype SequenceNum = MkSequenceNum Word64
  deriving stock (Eq, Ord)
  deriving newtype (Show, Real, Num, Enum, Integral, Binary)

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
