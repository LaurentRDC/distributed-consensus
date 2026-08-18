{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Network.Consensus.Raft.Log
  ( -- * Log
    Log (lSnapshotMetadata),
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

import Data.Sequence (Seq)
import qualified Data.Sequence as Seq
import GHC.Generics (Generic)
import Network.Consensus.Raft.Domain (LogIndex, SnapshotMetadata (..), Term)

-- | 'RelativeIndex' is a relative position within the 'Log'. It is
-- specified with respect to the snapshot.
newtype RelativeLogIndex = RelativeLogIndex LogIndex
  deriving stock (Generic, Eq, Ord, Show)
  deriving newtype (Real, Enum, Num, Integral)

data Log node state entry = Log
  { lEntries :: !(Seq entry),
    lSnapshotMetadata :: !SnapshotMetadata
  }

relativeIndex :: Log node state entry -> LogIndex -> RelativeLogIndex
relativeIndex (Log _ ((SnapshotMetadata !lastIx _))) !ix = RelativeLogIndex $ ix - lastIx

absoluteIndex :: Log node state entry -> RelativeLogIndex -> LogIndex
absoluteIndex (Log _ ((SnapshotMetadata !lastIx _))) (RelativeLogIndex ix) = ix + lastIx

append :: Log node state entry -> entry -> Log node state entry
Log es sn `append` newEntry = Log (es Seq.|> newEntry) sn

extend :: Log node state entry -> Seq entry -> Log node state entry
Log es sn `extend` newEntries = Log (es Seq.>< newEntries) sn

keepEntriesUpTo :: Log node state entry -> LogIndex -> Log node state entry
log'@(Log es sn@((SnapshotMetadata lastIx _))) `keepEntriesUpTo` ix
  | ix <= lastIx = Log Seq.empty sn
  | otherwise = Log (Seq.take (fromIntegral $ relativeIndex log' ix) es) sn

data Lookup a
  = LogIndexInSnapshot SnapshotMetadata
  | Found a
  | NotFound
  deriving (Functor)

(!?) :: Log node state entry -> LogIndex -> Lookup entry
(!?) log'@(Log es meta@(SnapshotMetadata lst _)) ix
  | ix <= lst = LogIndexInSnapshot meta
  | otherwise =
      maybe NotFound Found $
        es Seq.!? fromIntegral (relativeIndex log' ix)

logEntries :: Log node state entry -> Seq entry
logEntries = lEntries

newLog :: Log node state entry
newLog = Log mempty (SnapshotMetadata 0 0)

lastLogIndex :: Log node state entry -> LogIndex
lastLogIndex (Log entries ((SnapshotMetadata lastSnapshotIndex _))) =
  lastSnapshotIndex + fromIntegral (Seq.length entries)

lastLogInfo :: Log node state (Term, entry) -> (LogIndex, Term)
lastLogInfo (Log Seq.Empty (SnapshotMetadata ix t)) = (ix, t)
lastLogInfo log'@(Log (_ Seq.:|> (lastEntryTerm, _)) _) = (lastLogIndex log', lastEntryTerm)

applySnapshot :: SnapshotMetadata -> Log node state entry -> Log node state entry
applySnapshot newSnapshot@((SnapshotMetadata absoluteLastIndex _)) log' =
  let relativeLastIndex = relativeIndex log' absoluteLastIndex
   in Log (Seq.drop (fromIntegral relativeLastIndex) (lEntries log')) newSnapshot
