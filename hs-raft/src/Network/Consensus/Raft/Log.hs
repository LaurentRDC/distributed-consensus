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
  { lEntries :: !(Seq (Term, entry)),
    lSnapshotMetadata :: !SnapshotMetadata,
    -- Metadata stored for performance reasons
    lLastIndex :: !LogIndex,
    lLastTerm :: !Term
  }

relativeIndex :: Log node state entry -> LogIndex -> RelativeLogIndex
relativeIndex (Log _ ((SnapshotMetadata !lastIx _)) _ _) !ix = RelativeLogIndex $ ix - lastIx
{-# INLINEABLE relativeIndex #-}

absoluteIndex :: Log node state entry -> RelativeLogIndex -> LogIndex
absoluteIndex (Log _ ((SnapshotMetadata !lastIx _)) _ _) (RelativeLogIndex !ix) = ix + lastIx
{-# INLINEABLE absoluteIndex #-}

append :: Log node state entry -> (Term, entry) -> Log node state entry
Log es sn lst _ `append` (!term, !newEntry) = Log (es Seq.|> (term, newEntry)) sn (succ lst) term
{-# INLINEABLE append #-}

extend :: Log node state entry -> Seq (Term, entry) -> Log node state entry
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

keepEntriesUpTo :: Log node state entry -> LogIndex -> Log node state entry
log'@(Log es sn@((SnapshotMetadata lastIx _)) lst lt) `keepEntriesUpTo` ix
  | ix <= lastIx = Log Seq.empty sn lst lt
  | otherwise = Log (Seq.take (fromIntegral $ relativeIndex log' ix) es) sn lst lt

data Lookup a
  = LogIndexInSnapshot SnapshotMetadata
  | Found a
  | NotFound
  deriving (Functor)

(!?) :: Log node state entry -> LogIndex -> Lookup (Term, entry)
(!?) log'@(Log es meta@(SnapshotMetadata lst _) _ _) ix
  | ix <= lst = LogIndexInSnapshot meta
  | otherwise =
      maybe NotFound Found $
        es Seq.!? fromIntegral (relativeIndex log' ix)

logEntries :: Log node state entry -> Seq (Term, entry)
logEntries = lEntries

newLog :: Log node state entry
newLog = Log mempty (SnapshotMetadata 0 0) 0 0

lastLogIndex :: Log node state entry -> LogIndex
lastLogIndex = lLastIndex
{-# INLINE lastLogIndex #-}

lastLogInfo :: Log node state entry -> (LogIndex, Term)
lastLogInfo (Log _ _ lix lt) = (lix, lt)
{-# INLINE lastLogInfo #-}

applySnapshot :: SnapshotMetadata -> Log node state entry -> Log node state entry
applySnapshot newSnapshot@((SnapshotMetadata absoluteLastIndex _)) log' =
  let relativeLastIndex = relativeIndex log' absoluteLastIndex
   in Log
        (Seq.drop (fromIntegral relativeLastIndex) (lEntries log'))
        newSnapshot
        (lLastIndex log')
        (lLastTerm log')
