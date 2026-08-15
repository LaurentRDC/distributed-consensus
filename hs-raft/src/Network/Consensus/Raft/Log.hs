{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Network.Consensus.Raft.Log
  ( LogIndex,
    Snapshot (..),
    SnapshotMetadata (..),

    -- * Log
    Log (lSnapshot),
    newLog,

    -- ** Reading the log
    logEntries,
    lastLogIndex,
    lastLogInfo,
    (!?),
    Lookup (..),
    relativeIndex,
    absoluteIndex,

    -- ** Modifying the log
    append,
    extend,
    keepEntriesUpTo,
    applySnapshot,
  )
where

import Data.Binary (Binary)
import Data.Int (Int64)
import Data.Sequence (Seq)
import qualified Data.Sequence as Seq
import Data.Set (Set)
import GHC.Generics (Generic)
import Network.Consensus.Raft.Domain (ClusterConfiguration (..), Term)

-- | 'LogIndex' is an absolute position within the 'Log'.
--
-- Note that the 'Log' may not have the elements associated with a given
-- 'LogIndex' due to compaction.
newtype LogIndex = LogIndex Int64
  deriving stock (Generic, Eq, Ord, Show)
  deriving newtype (Real, Enum, Num, Binary, Integral)

-- | 'RelativeIndex' is a relative position within the 'Log'. It is
-- specified with respect to the snapshot.
newtype RelativeLogIndex = RelativeLogIndex LogIndex
  deriving stock (Generic, Eq, Ord, Show)
  deriving newtype (Real, Enum, Num, Integral)

data Log node state entry = Log
  { lEntries :: !(Seq entry), -- TODO: how about Map LogIndex entry?
    lSnapshot :: !(Snapshot node state)
  }

relativeIndex :: Log node state entry -> LogIndex -> RelativeLogIndex
relativeIndex (Log _ (Snapshot (SnapshotMetadata !lastIx _) _ _)) !ix = RelativeLogIndex $ ix - lastIx

absoluteIndex :: Log node state entry -> RelativeLogIndex -> LogIndex
absoluteIndex (Log _ (Snapshot (SnapshotMetadata !lastIx _) _ _)) (RelativeLogIndex ix) = ix + lastIx

append :: Log node state entry -> entry -> Log node state entry
Log es sn `append` newEntry = Log (es Seq.|> newEntry) sn

extend :: Log node state entry -> Seq entry -> Log node state entry
Log es sn `extend` newEntries = Log (es Seq.>< newEntries) sn

keepEntriesUpTo :: Log node state entry -> LogIndex -> Log node state entry
log'@(Log es sn@(Snapshot (SnapshotMetadata lastIx _) _ _)) `keepEntriesUpTo` ix
  | ix <= lastIx = Log Seq.empty sn
  | otherwise = Log (Seq.take (fromIntegral $ relativeIndex log' ix) es) sn

data Lookup a
  = LogIndexInSnapshot SnapshotMetadata
  | Found a
  | NotFound
  deriving (Functor)

(!?) :: Log node state entry -> LogIndex -> Lookup entry
(!?) log'@(Log es (Snapshot meta@(SnapshotMetadata lst _) _ _)) ix
  | ix <= lst = LogIndexInSnapshot meta
  | otherwise =
      maybe NotFound Found $
        es Seq.!? fromIntegral (relativeIndex log' ix)

logEntries :: Log node state entry -> Seq entry
logEntries = lEntries

newLog :: state -> Set node -> Log node state entry
newLog st cluster = Log mempty (Snapshot (SnapshotMetadata 0 0) st (Simple cluster))

lastLogIndex :: Log node state entry -> LogIndex
lastLogIndex (Log entries (Snapshot (SnapshotMetadata lastSnapshotIndex _) _ _)) =
  lastSnapshotIndex + fromIntegral (Seq.length entries)

lastLogInfo :: Log node state (Term, entry) -> (LogIndex, Term)
lastLogInfo (Log Seq.Empty (Snapshot (SnapshotMetadata ix t) _ _)) = (ix, t)
lastLogInfo log'@(Log (_ Seq.:|> (lastEntryTerm, _)) _) = (lastLogIndex log', lastEntryTerm)

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

applySnapshot :: Snapshot node state -> Log node state entry -> Log node state entry
applySnapshot newSnapshot@(Snapshot (SnapshotMetadata absoluteLastIndex _) _ _) log' =
  let relativeLastIndex = relativeIndex log' absoluteLastIndex
   in Log (Seq.drop (fromIntegral relativeLastIndex) (lEntries log')) newSnapshot
