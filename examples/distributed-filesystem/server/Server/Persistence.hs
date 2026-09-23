{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module Server.Persistence (withServerPersistence) where

import Control.Distributed.Process (NodeId, Process)
import Control.Exception (SomeException, try)
import Control.Monad (void, when)
import Control.Monad.IO.Class (liftIO)
import qualified Data.Binary as Binary
import Data.Binary.Get (runGetOrFail)
import qualified Data.ByteString.Lazy as BSL
import Data.Functor ((<&>))
import Data.IORef (modifyIORef', newIORef, readIORef, writeIORef)
import Data.List.NonEmpty (nonEmpty)
import Distributed.Consensus.Raft (Persistence (..))
import FileSystem (FileSystem)
import Server.StateMachine (FileSystemCommand)
import System.Directory.OsPath (removeFile)
import System.File.OsPath (withBinaryFile)
import qualified System.File.OsPath as IO
import System.IO (IOMode (..))
import System.IO.WAL (WALConfig (..), activeSegment, append, appendMany, binaryWALCodec, replaySegment, rotate, withWriteAheadLog)
import System.OsPath (OsPath, unsafeEncodeUtf, (</>))
import qualified System.OsPath as OsPath

withServerPersistence :: OsPath -> (Persistence FileSystemCommand NodeId FileSystem Process -> Process a) -> Process a
withServerPersistence dir f = do
  withWriteAheadLog
    binaryWALCodec
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
                writeLogEntry = \_processId -> maybe (pure ()) (appendMany wal) . nonEmpty . map Right,
                readTerm = readTermFromFile,
                writeTerm = writeTermToFile,
                readVotedFor = readVotedForFromFile,
                writeVotedFor = writeVotedForToFile,
                readSnapshot = readSnapshotFromWAL wal,
                writeSnapshot = writeSnapshotToWAL wal
              }
       in f persistence
  where
    readLogEntries wal minLogIndex = do
      -- We assume that we'll never need to read log entries before the
      -- latest snapshot, AND that every WAL file starts with a snapshot,
      -- so we only need to read the last WAL segment
      ref <- liftIO $ newIORef []
      seg <- activeSegment wal
      -- We prepend entries to the list in the IOref,
      -- so we need to reverse before returning
      replaySegment
        wal
        seg
        ( \case
            Left _ -> pure ()
            Right (logIx, term, entry) ->
              when (logIx >= minLogIndex) (modifyIORef' ref ((term, entry) :))
        )
      reverse <$> liftIO (readIORef ref)

    termFile = dir </> unsafeEncodeUtf "term"
    voteFile term = dir </> unsafeEncodeUtf "vote" <> OsPath.unsafeEncodeUtf (show (toInteger term))

    readTermFromFile _node = liftIO $ try (IO.readFile termFile) >>= either (\(_ :: SomeException) -> pure 0) (pure . Binary.decode)
    writeTermToFile _node = liftIO . IO.writeFile termFile . Binary.encode

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

    writeVotedForToFile _node term Nothing = liftIO $ void $ try @SomeException $ removeFile (voteFile term)
    writeVotedForToFile _node term (Just v) =
      liftIO
        $ void
        $ withBinaryFile
          (voteFile term)
          WriteMode
        $ \h -> BSL.hPut h (Binary.encode v)

    readSnapshotFromWAL wal _node = do
      ref <- liftIO $ newIORef Nothing
      seg <- activeSegment wal
      replaySegment
        wal
        seg
        ( \case
            Left snap -> writeIORef ref (Just snap)
            Right _ -> pure ()
        )
      liftIO (readIORef ref)

    writeSnapshotToWAL wal _node snap = do
      _ <- rotate wal
      append wal (Left snap)
