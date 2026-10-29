{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- I like <&>
{- HLINT ignore "Functor law" -}
module Distributed.Consensus.Raft.WAL
  ( withPersistence,

    -- * Codecs
    WALCodec (..),

    -- ** Built-in codec
    binaryWALCodec,
  )
where

import Control.Exception (SomeException, try)
import Control.Monad (when)
import Control.Monad.Catch (MonadMask)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.Binary (Binary)
import qualified Data.Binary as Binary
import Data.Binary.Get (runGetOrFail)
import qualified Data.ByteString.Lazy as BSL
import Data.Foldable (traverse_)
import Data.Functor ((<&>))
import Data.IORef (modifyIORef', newIORef, readIORef, writeIORef)
import Data.List.NonEmpty (nonEmpty)
import qualified Data.Map.Strict as Map
import Distributed.Consensus.Raft (LogEntry, LogIndex, Persistence (..), Snapshot, Term)
import GHC.Generics (Generic)
import System.File.OsPath (withBinaryFile)
import qualified System.File.OsPath as IO
import System.IO (IOMode (..))
import qualified System.IO.File.Durable as Durable
import System.IO.WAL (WALCodec (..), WALConfig (..), activeSegment, append, appendMany, binaryWALCodec, replaySegment, rotate, withWriteAheadLog)
import System.OsPath (OsPath, unsafeEncodeUtf, (</>))
import qualified System.OsPath as OsPath

data RaftRecord entry node state
  = SnapshotRecord (Snapshot node state)
  | LogRecord (Term, LogIndex, LogEntry node entry)
  | -- | Truncating the write-ahead log is done by changing
    -- how the read-back is done
    TruncateFrom LogIndex
  deriving (Generic)

instance (Binary entry, Binary node, Binary state) => Binary (RaftRecord entry node state)

-- | A function to run a Raft cluster with the provided 'Persistence'
-- instance based on a disk-backed write-ahead log.
--
-- This function takes care of all the details related to setting
-- up the log directory, rotating log files, and persisting
-- small pieces of data (e.g. terms and votes) separately
-- from the log files.
--
-- Log files are rotated on Raft server snapshot. This means that every
-- log file is standalone; there is no need to keep any other file
-- than the most current one, except perhaps for auditing.
--
-- If the directory where log files should be stored doesn't exist,
-- it will be created (including its parents if needed).
withPersistence ::
  ( MonadIO m,
    Binary node,
    MonadMask m
  ) =>
  -- | Directory where files should be stored. This directory need not be empty. The
  -- Raft server will restore its state based on the contents of the directory.
  OsPath ->
  -- | Codec to encode and decode data to/from the log. If you don't want to choose,
  -- just use 'binaryWALCodec'. If you want to squeeze as much performance as you can,
  -- you will need to created your own 'WALCodec'.
  --
  -- This codec is only relevant for data in the write-ahead log. Smaller pieces of data
  -- (e.g. a server's current term or commit index) is serialized to small files
  -- using the 'Binary' instance.
  WALCodec (RaftRecord entry node state) ->
  -- | Continuation
  (Persistence entry node state m -> m a) ->
  m a
withPersistence dir codec f = do
  withWriteAheadLog
    codec
    ( WALConfig
        { maxSegmentBytes = maxBound, -- Rotation happens on snapshot
          directory = dir </> unsafeEncodeUtf "wal"
        }
    )
    $ \wal ->
      let persistence =
            -- In our persistence scheme, snapshots and log entries are stored in the write-ahead log,
            -- while votes and terms are stored in local files. This is to show how we can mix things
            -- inside the same persistence layer. In practice, you might want to put everything in
            -- the write-ahead log.
            --
            -- The write-ahead log is naturally broken down into segments, or files. One invariant
            -- we will enforce below: every segment, except the first one, begins with a snapshot.
            -- When writing a snapshot to the WAL, we first rotate to a new segment.
            --
            -- This means that we only ever need to read back the active segment (i.e. latest one)
            Persistence
              { readLogEntriesFrom = \_ -> readLogEntries wal,
                writeLogEntry = \_processId -> traverse_ (appendMany wal) . nonEmpty . map LogRecord,
                truncateLogEntriesFrom = \_processId -> append wal . TruncateFrom,
                readTerm = readTermFromFile,
                writeTerm = writeTermToFile,
                readVotedFor = readVotedForFromFile,
                writeVotedFor = writeVotedForToFile,
                readSnapshot = readSnapshotFromWAL wal,
                writeSnapshot = writeSnapshotToWAL wal,
                readCommitIndex = readCommitIndexFromFile,
                writeCommitIndex = writeCommitIndexToFile
              }
       in f persistence
  where
    readLogEntries wal minLogIndex = do
      -- We assume that we'll never need to read log entries before the
      -- latest snapshot, AND that every WAL file starts with a snapshot,
      -- so we only need to read the last WAL segment
      ref <- liftIO $ newIORef mempty
      seg <- activeSegment wal
      -- We deliberately use a Map since WAL doesn't support overwriting.
      -- "Overwriting" the WAL is done by writing the latest record instead.
      replaySegment
        wal
        seg
        ( \case
            SnapshotRecord _ -> pure ()
            LogRecord (term, logIx, entry) ->
              when (logIx >= minLogIndex) (modifyIORef' ref (Map.insert (term, logIx) entry))
            TruncateFrom logIndex ->
              modifyIORef' ref (Map.filterWithKey (\(_term, ix) _entry -> ix < logIndex))
        )
      liftIO (readIORef ref)
        <&> Map.toAscList
        <&> map (\((t, ix), e) -> (t, ix, e))

    termFile = dir </> unsafeEncodeUtf "term"
    voteFile term = dir </> unsafeEncodeUtf "vote" <> OsPath.unsafeEncodeUtf (show (toInteger term))
    commitIndexFile = dir </> unsafeEncodeUtf "commit_index"

    readTermFromFile _node = liftIO $ try (IO.readFile termFile) >>= either (\(_ :: SomeException) -> pure 0) (pure . Binary.decode)

    writeTermToFile _node = writeDurably termFile

    readCommitIndexFromFile _node = liftIO $ try (IO.readFile commitIndexFile) >>= either (\(_ :: SomeException) -> pure 0) (pure . Binary.decode)
    writeCommitIndexToFile _node = writeDurably commitIndexFile

    readVotedForFromFile _node term =
      liftIO $
        try
          ( withBinaryFile (voteFile term) ReadMode $ \h ->
              BSL.hGetContents h
                <&> runGetOrFail Binary.get
                >>= \case
                  Left (_, _, errmsg) -> fail errmsg
                  Right (_, _, v) -> pure v
          )
          >>= either (\(_ :: SomeException) -> pure Nothing) (pure . Just)

    writeVotedForToFile _node term Nothing = liftIO $ Durable.removeFileDurable (voteFile term)
    writeVotedForToFile _node term (Just v) = writeDurably (voteFile term) v

    readSnapshotFromWAL wal _node = do
      ref <- liftIO $ newIORef Nothing
      seg <- activeSegment wal
      replaySegment
        wal
        seg
        ( \case
            SnapshotRecord snap -> writeIORef ref (Just snap)
            LogRecord {} -> pure ()
            TruncateFrom {} -> pure ()
        )
      liftIO (readIORef ref)

    writeSnapshotToWAL wal _node snap = do
      _ <- rotate wal
      append wal (SnapshotRecord snap)

writeDurably :: (MonadIO m, Binary a) => OsPath -> a -> m ()
writeDurably file =
  liftIO
    . Durable.writeFileAtomic file
    . BSL.toStrict
    . Binary.encode
