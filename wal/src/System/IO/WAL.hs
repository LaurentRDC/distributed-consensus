{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE TupleSections #-}

-- I like using <&>, sue me hlint
{- HLINT ignore "Functor law" -}

-- |
-- Module      : System.IO.WAL
-- Description : A generic, durable, rotating write-ahead log.
module System.IO.WAL
  ( -- * Write-ahead log
    withWriteAheadLog,
    open,
    close,
    WriteAheadLog,

    -- ** Writing
    append,
    appendMany,

    -- ** Manual rotation
    rotate,

    -- ** Reading
    Segment,
    segmentPath,
    segments,
    activeSegment,
    replayAll,
    replaySegment,

    -- ** Configuration
    WALCodec (..),
    binaryWALCodec,
    WALConfig (..),
  )
where

import Control.Concurrent.MVar (MVar, modifyMVar, newMVar, withMVar)
import Control.Monad (forM_, unless, void, when)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.Attoparsec.ByteString (Parser)
import qualified Data.Attoparsec.ByteString as Parser
import Data.Bifunctor (bimap)
import Data.Binary (Binary, decodeOrFail, put)
import Data.Binary.Put (execPut)
import Data.Bits (Bits (shiftL, (.|.)))
import Data.ByteString (StrictByteString)
import qualified Data.ByteString as B
import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder as BB
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Unsafe as BSU
import Data.Digest.CRC32 (crc32)
import Data.Function ((&))
import Data.Functor ((<&>))
import Data.IORef
  ( IORef,
    modifyIORef',
    newIORef,
    readIORef,
    writeIORef,
  )
import Data.Int (Int64)
import Data.List (sortOn)
import Data.List.NonEmpty (NonEmpty)
import qualified Data.List.NonEmpty as NonEmpty
import Data.Maybe (catMaybes)
import Data.Set (Set)
import qualified Data.Set as Set
import Data.Word (Word32)
import Foreign.Ptr (castPtr)
import System.Directory.OsPath
  ( createDirectoryIfMissing,
    doesFileExist,
    listDirectory,
  )
import System.File.OsPath (withBinaryFile)
import System.IO
  ( IOMode (ReadMode, WriteMode),
  )
import System.IO.File.Durable (FHandle)
import qualified System.IO.File.Durable as WALFile
import System.OsPath (OsPath, decodeFS, encodeFS, (</>))
import Text.Printf (printf)
import Text.Read (readMaybe)

-- | Represents a reference to a write-ahead log.
--
-- Append to the log using 'append'. Once 'append' returns, the
-- log has been updated durably.
-- 'append' is a slow operation. You can batch appends using 'appendMany',
-- which will commit all appends at once, atomically.
--
-- A write-ahead log is really a directory of files, called /segments/.
-- When the log gets long enough, the active file rotates automatically.
-- This can also be done manually using 'rotate'. You can list
-- existing segments using 'segments'.
--
-- You can read back the entire log in sequential order using 'replayAll'.
-- You can also read back specific segments using 'replaySegment'.
--
-- Using the same 'WriteAheadLog' from multiple threads is safe.
data WriteAheadLog a = WriteAheadLog
  { walCodec :: !(WALCodec a),
    walConfig :: !WALConfig,
    walHandle :: !(MVar WALHandle),
    walActiveSegment :: !(IORef Segment),
    -- | bytes written to current segment
    walWritten :: !(IORef Int64)
  }

-- | How to encode and decode write-ahead log entries.
--
-- A 'binaryWALCodec' is provided if you want to reuse instances from
-- the 'binary' package.
data WALCodec a = WALCodec
  { encodeEntry :: a -> BB.Builder,
    decodeEntry :: StrictByteString -> Either String a
  }

-- | 'WALCodec' making use of the 'Binary' typeclass.
--
-- This is often convenient, but less performant, than alternatives.
binaryWALCodec :: (Binary a) => WALCodec a
binaryWALCodec =
  let trd (_, _, !x) = x
   in WALCodec
        { encodeEntry = execPut . put,
          decodeEntry =
            bimap
              trd
              trd
              . decodeOrFail
              . BL.fromStrict
        }

-- | Configuration
data WALConfig = WALConfig
  { -- | Rotate to a new segment once the current one reaches this size.
    maxSegmentBytes :: !Int64,
    -- | Directory where to write write-ahead log files. This directory need
    -- not be empty.
    directory :: !OsPath
  }

-- | Open a 'WriteAheadLog' based on an input configuration, and a codec
-- to encode and decode entries.
--
-- If the configuration points to an existing directory, the 'WriteAheadLog'
-- will pick back up from where it last stopped. To reset a 'WriteAheadLog',
-- its associated directory must be emptied.
withWriteAheadLog ::
  (MonadIO m) =>
  WALCodec a ->
  WALConfig ->
  (WriteAheadLog a -> m b) ->
  m b
withWriteAheadLog codec config act = do
  wal <- liftIO (open codec config)
  r <- act wal
  liftIO $ close wal
  pure r

open :: WALCodec a -> WALConfig -> IO (WriteAheadLog a)
open codec config = do
  createDirectoryIfMissing True (directory config)

  segs <- listSegmentsFromDir (directory config)
  let latest = case map fst segs of
        [] -> 0
        ns -> maximum ns

  path <- (directory config </>) <$> segmentPath latest

  exists <- doesFileExist path
  unless exists $ withBinaryFile path WriteMode (const (pure ()))

  h <- WALFile.open path
  sz <- WALFile.size h
  WriteAheadLog
    codec
    config
    <$> newMVar (WALHandle h path)
    <*> newIORef latest
    <*> newIORef (fromIntegral sz)

close :: WriteAheadLog a -> IO ()
close wal = withMVar (walHandle wal) $ \(WALHandle h _) -> WALFile.close h

-- | Entries in the Log are structured as:
--
-- * 4 bytes for the length of the payload, encoded as a little-endian Word32;
-- * 4 bytes for the checksum (crc32), encoded as a little-endian Word32;
-- * N payload bytes
frame :: BB.Builder -> BB.Builder
frame payloadB =
  let payload = BL.toStrict (BB.toLazyByteString payloadB)
      len = fromIntegral (BS.length payload) :: Word32
      crc = crc32 payload
   in BB.word32LE len <> BB.word32LE crc <> BB.byteString payload

anyWord32le :: Parser Word32
anyWord32le = Parser.take 4 <&> pack . BS.reverse
  where
    pack :: (Bits a, Num a) => StrictByteString -> a
    pack = B.foldl' (\n h -> (n `shiftL` 8) .|. fromIntegral h) 0

frameGet :: WALCodec a -> Parser a
frameGet (WALCodec _ decode) = do
  len <- anyWord32le
  checksum <- anyWord32le
  bytes <- Parser.take (fromIntegral len)
  if crc32 bytes /= checksum
    then fail "WAL: checksum mismatch (torn or corrupted write)"
    else
      decode bytes
        & either
          (\e -> fail ("WAL: decode error: " ++ e))
          pure

-- 'ensureFileDurable' requires a known 'OsPath',
-- which we must keep in sync with a 'Handle'
data WALHandle = WALHandle !FHandle !OsPath

newtype Segment = MkSegment Word32
  deriving (Show, Eq)
  deriving newtype (Ord, Enum, Integral, Real, Num, Read)

segPrefix :: String
segPrefix = "wal-"

segSuffix :: String
segSuffix = ".log"

segmentPath :: Segment -> IO OsPath
segmentPath (MkSegment n) = encodeFS $ printf "wal-%06d.log" (fromIntegral n :: Int)

-- | Parse a segment number out of a file name produced by 'segmentName'.
-- Returns 'Nothing' for anything that isn't one of our segment files, so
-- stray files in the directory are safely ignored.
parseSegment :: FilePath -> Maybe Segment
parseSegment f
  | take (length segPrefix) f == segPrefix,
    drop (length f - length segSuffix) f == segSuffix =
      readMaybe (drop (length segPrefix) (take (length f - length segSuffix) f))
  | otherwise =
      Nothing

listSegmentsFromDir :: (MonadIO m) => OsPath -> m [(Segment, OsPath)]
listSegmentsFromDir dir =
  liftIO $
    listDirectory dir
      >>= traverse decodeFS
      <&> fmap (\fp -> (,fp) <$> parseSegment fp)
      <&> catMaybes
      <&> sortOn fst
      >>= traverse (\(s, fs') -> (s,) <$> encodeFS fs')

-- | List all segments relevant to this write-ahead log.
--
-- This can be used to replay the appropriate segments.
-- See 'activeSegment' for the most recent segment.
segments :: (MonadIO m) => WriteAheadLog a -> m (Set Segment)
segments wal =
  listSegmentsFromDir (directory (walConfig wal))
    <&> fmap fst
    <&> Set.fromList

-- | Return the active segment of the write-ahead log
activeSegment :: (MonadIO m) => WriteAheadLog a -> m Segment
activeSegment = liftIO . readIORef . walActiveSegment

-- | Append a single entry to the log. Once 'append' returns,
-- the entry is durably stored on disk.
--
-- Prefer to use 'appendMany' as much as possible, as it is much,
-- much faster.
--
-- Appending to the log may result in the log file rotating
-- to a new file.
append :: (MonadIO m) => WriteAheadLog a -> a -> m ()
append wal = appendMany wal . NonEmpty.singleton

-- | Append a batch of entries to the log. Once 'appendMany'
-- returns, the entries are durably stored on disk.
--
-- Appending to the log may result in the log file rotating
-- to a new file.
appendMany :: (MonadIO m) => WriteAheadLog a -> NonEmpty a -> m ()
appendMany wal xs = liftIO $ do
  withMVar (walHandle wal) $ \(WALHandle h _) -> do
    let builder = foldMap (frame . encodeEntry (walCodec wal)) xs
        bytes = BL.toStrict (BB.toLazyByteString builder)
        nBytes = fromIntegral (BS.length bytes) :: Int64

    _count <- BSU.unsafeUseAsCString bytes $ \ptr -> WALFile.write h (castPtr ptr) (fromIntegral nBytes)
    -- TODO: what do I do if '_count' is smaller than expected?

    WALFile.flush h
    modifyIORef' (walWritten wal) (+ nBytes)

    written <- readIORef (walWritten wal)
    when
      (written >= maxSegmentBytes (walConfig wal))
      (void $ rotate wal)

-- | Close the current segment and open a fresh, empty one.
rotate :: (MonadIO m) => WriteAheadLog a -> m Segment
rotate wal = liftIO $ do
  modifyMVar (walHandle wal) $ \(WALHandle h _) -> do
    WALFile.flush h
    WALFile.close h

    n' <- (+ 1) <$> readIORef (walActiveSegment wal)
    path <- (directory (walConfig wal) </>) <$> segmentPath n'

    -- The next segment file is overwritten
    withBinaryFile path WriteMode (const (pure ()))

    h'' <- WALFile.open path

    writeIORef (walActiveSegment wal) n'
    writeIORef (walWritten wal) 0

    pure (WALHandle h'' path, n')

-- | Stream the contents of a write-ahead log segment.
--
-- See 'segments' to fetch available segments for a given 'WriteAheadLog'
--
-- It is safe to replay the segment to which we are currently writing;
-- however, 'replaySegment' will block until everything has been replayed,
-- such that one cannot read-and-write simultaneously.
replaySegment ::
  (MonadIO m) =>
  WriteAheadLog a ->
  Segment ->
  (a -> IO ()) ->
  m ()
replaySegment wal segment onEntry = liftIO $ do
  active <- activeSegment wal
  -- If we're trying to naively replay the current active segment,
  -- then we'll get a 'resource busy' error since the file is
  -- open for appending.
  --
  -- In this case, we close the handle, replay, then re-open the handle.
  --
  -- Note that this is safe since 'replaySegment' locks access to the
  -- WALHandle while replaying
  if active == segment
    then do
      modifyMVar (walHandle wal) $ \(WALHandle h fp) -> do
        WALFile.close h
        replayUnusedSegment
        h' <- WALFile.open fp
        pure (WALHandle h' fp, ())
    else replayUnusedSegment
  where
    replayUnusedSegment = do
      p <- segmentPath segment
      withBinaryFile
        (directory (walConfig wal) </> p)
        ReadMode
        $ \h -> go h (Parser.parse parseChunk BS.empty)

    parseChunk = Parser.many' (frameGet (walCodec wal))

    go h (Parser.Done leftover values) = mapM_ onEntry values >> go h (Parser.parse parseChunk leftover)
    go h (Parser.Partial continue) = do
      bytes <- liftIO $ BS.hGet h (64 * 1024)
      if BS.null bytes
        then case continue BS.empty of
          -- We allow leftover specifically in case the WAL file
          -- was partially-written
          Parser.Done _leftover values -> do
            mapM_ onEntry values
          Parser.Partial _ -> fail "Unexpected end-of-file"
          -- TODO: why
          Parser.Fail _ ctx msg -> fail ("Parser error: " <> msg <> ". Context: " <> show ctx)
        else
          go h (continue bytes)
    go _ (Parser.Fail _ _ msg) = fail ("Parser error: " <> msg)

-- | Replay every segment in the log directory, oldest first, feeding each
-- decoded entry to the given callback in order.
replayAll :: (MonadIO m) => WriteAheadLog a -> (a -> IO ()) -> m ()
replayAll wal onEntry = do
  segs <- listSegmentsFromDir (directory (walConfig wal))
  forM_ segs $ \(seg, _) -> replaySegment wal seg onEntry
