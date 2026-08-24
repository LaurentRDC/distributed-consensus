{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Distributed.Consensus.Raft.Log
  ( -- * Log
    Log (lEntries, lSnapshotMetadata),
    buildLog,
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

import Data.Maybe (fromMaybe)
import Data.Sequence (Seq)
import qualified Data.Sequence as Seq
import Distributed.Consensus.Raft.Domain (LogIndex, SnapshotMetadata (..), Term)
import GHC.Generics (Generic)

-- | 'RelativeIndex' is a relative position within the 'Log'. It is
-- specified with respect to the snapshot.
newtype RelativeLogIndex = RelativeLogIndex LogIndex
  deriving stock (Generic, Eq, Ord, Show)
  deriving newtype (Real, Enum, Num, Integral)

data Log node entry = Log
  { lEntries :: !(Seq (Term, entry)),
    lSnapshotMetadata :: !SnapshotMetadata,
    -- Metadata stored for performance reasons
    lLastIndex :: !LogIndex,
    lLastTerm :: !Term
  }
  deriving (Eq, Show)

buildLog :: Seq (Term, entry) -> Maybe SnapshotMetadata -> Log node entry
buildLog entries mMetadata =
  let metadata = fromMaybe (SnapshotMetadata 0 0) mMetadata
   in Log
        { lEntries = entries,
          lSnapshotMetadata = metadata,
          lLastIndex = smLastIncludedIndex metadata + fromIntegral (length entries),
          lLastTerm = case Seq.viewr entries of
            Seq.EmptyR -> smLastIncludedTerm metadata
            _ Seq.:> (lastTerm, _) -> lastTerm
        }

relativeIndex :: Log node entry -> LogIndex -> RelativeLogIndex
relativeIndex (Log _ ((SnapshotMetadata !lastIx _)) _ _) !ix = RelativeLogIndex $ ix - lastIx
{-# INLINEABLE relativeIndex #-}

absoluteIndex :: Log node entry -> RelativeLogIndex -> LogIndex
absoluteIndex (Log _ ((SnapshotMetadata !lastIx _)) _ _) (RelativeLogIndex !ix) = ix + lastIx
{-# INLINEABLE absoluteIndex #-}

append :: Log node entry -> (Term, entry) -> Log node entry
Log es sn lst _ `append` (!term, !newEntry) = Log (es Seq.|> (term, newEntry)) sn (succ lst) term
{-# INLINEABLE append #-}

extend :: Log node entry -> Seq (Term, entry) -> Log node entry
Log es sn lst lt `extend` !newEntries =
  Log
    (es Seq.>< newEntries)
    sn
    (lst + fromIntegral (Seq.length newEntries))
    newLastTerm
  where
    newLastTerm = case Seq.viewr newEntries of
      Seq.EmptyR -> lt
      (_ Seq.:> (t, _)) -> t
{-# INLINEABLE extend #-}

-- | Truncate the log so that it retains only the entries up to, and including,
-- the absolute index @ix@.
keepEntriesUpTo :: Log node entry -> LogIndex -> Log node entry
log'@(Log es sn@((SnapshotMetadata lastIx lastSnapshotTerm)) lst _) `keepEntriesUpTo` ix
  -- Nothing to truncate
  | ix >= lst = log'
  | ix <= lastIx = Log Seq.empty sn lastIx lastSnapshotTerm
  | otherwise =
      let kept = Seq.take (fromIntegral $ relativeIndex log' ix) es
          term = case Seq.viewr kept of
            Seq.EmptyR -> lastSnapshotTerm
            _ Seq.:> (keptLastTerm, _) -> keptLastTerm
       in Log kept sn ix term

data Lookup a
  = LogIndexInSnapshot SnapshotMetadata
  | Found a
  | NotFound
  deriving (Functor)

(!?) :: Log node entry -> LogIndex -> Lookup (Term, entry)
(!?) log'@(Log es meta@(SnapshotMetadata lst _) _ _) ix
  | ix <= lst = LogIndexInSnapshot meta
  | otherwise =
      maybe NotFound Found $
        -- Seq.!? is zero-based indexing, but the Raft log
        -- is traditionally one-based indexing, hence the -1 offset
        es Seq.!? (fromIntegral (relativeIndex log' ix) - 1)

logEntries :: Log node entry -> Seq (Term, entry)
logEntries = lEntries

newLog :: Log node entry
newLog = buildLog mempty Nothing

lastLogIndex :: Log node entry -> LogIndex
lastLogIndex = lLastIndex
{-# INLINE lastLogIndex #-}

lastLogInfo :: Log node entry -> (LogIndex, Term)
lastLogInfo (Log _ _ lix lt) = (lix, lt)
{-# INLINE lastLogInfo #-}

applySnapshot :: SnapshotMetadata -> Log node entry -> Log node entry
applySnapshot newSnapshot@((SnapshotMetadata absoluteLastIndex lastSnapshotTerm)) log' =
  let relativeLastIndex = relativeIndex log' absoluteLastIndex
   in Log
        (Seq.drop (fromIntegral relativeLastIndex) (lEntries log'))
        newSnapshot
        (max absoluteLastIndex (lLastIndex log'))
        (max lastSnapshotTerm (lLastTerm log'))
