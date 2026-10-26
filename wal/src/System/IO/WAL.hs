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
    rawWALCodec,
    WALConfig (..),
  )
where

import Control.Concurrent.MVar (MVar, modifyMVar, newMVar, withMVar)
import Control.Exception
  ( SomeException,
    catch,
    displayException,
    evaluate,
    mask,
    mask_,
    throwIO,
    try,
  )
import Control.Monad (forM_, unless, void, when)
import Control.Monad.Catch (MonadMask, finally)
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
import Data.Digest.CRC32C (crc32c)
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
import Data.Word (Word32, Word64)
import Foreign.Ptr (castPtr)
import System.Directory.OsPath
  ( createDirectoryIfMissing,
    doesDirectoryExist,
    doesFileExist,
    listDirectory,
  )
import System.File.OsPath (withBinaryFile)
import System.IO
  ( Handle,
    IOMode (..),
    hSetFileSize,
  )
import System.IO.File.Durable (FHandle)
import qualified System.IO.File.Durable as WALFile
import System.OsPath (OsPath, decodeFS, encodeFS, takeDirectory, (</>))
import Text.Printf (printf)
import Text.Read (readMaybe)

-- | Represents a reference to a write-ahead log.
--
-- Append to the log using 'append'. Once 'append' returns, the
-- log has been updated durably.
-- 'append' is a slow operation. You can batch appends using 'appendMany',
-- which commits all appends at once, atomically.
--
-- If writing to the log ever fails, the log refuses any further writes
-- until it is reopened.
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
    walWritten :: !(IORef Int64),
    -- | In case an error has happened when trying to append, we must
    -- be able to share this with other threads.
    --
    -- TODO: is there a more elegant way to handle errors in a multi-
    -- threaded environment?
    walFailure :: !(IORef (Maybe SomeException))
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

-- | 'WALCodec' which doesn't decode payloads
rawWALCodec :: WALCodec StrictByteString
rawWALCodec =
  WALCodec
    { encodeEntry = BB.byteString,
      decodeEntry = Right
    }

-- | Configuration
data WALConfig = WALConfig
  { -- | Once the active segment reaches this size, the next append
    -- goes to a new segment.
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
--
-- This function is exception-safe, in that the log files will be
-- properly closed, even if the continuation throws an exception.
withWriteAheadLog ::
  (MonadIO m, MonadMask m) =>
  WALCodec a ->
  WALConfig ->
  (WriteAheadLog a -> m b) ->
  m b
withWriteAheadLog codec config act = do
  wal <- liftIO (open codec config)
  act wal `finally` liftIO (close wal)

-- | Open a write-ahead log.
--
-- If the directory where log files should be stored doesn't exist,
-- it will be created (including its parents if needed).
--
-- If active log file is torn, the partial log entry will be
-- removed such that the log is repaired.
open :: WALCodec a -> WALConfig -> IO (WriteAheadLog a)
open codec config = do
  createDirectoryDurably (directory config)

  segs <- listSegmentsFromDir (directory config)
  let latest = case map fst segs of
        [] -> 0
        ns -> maximum ns

  path <- (directory config </>) <$> segmentPath latest

  exists <- doesFileExist path
  if exists
    then withBinaryFile path ReadWriteMode $ \h -> do
      v <- validSize h
      hSetFileSize h (fromIntegral v)
    else do
      withBinaryFile path WriteMode (const (pure ()))
      WALFile.syncDirectory (directory config)

  h <- WALFile.open path
  sz <- WALFile.size h
  WriteAheadLog
    codec
    config
    <$> newMVar (WALHandle h path)
    <*> newIORef latest
    <*> newIORef (fromIntegral sz)
    <*> newIORef Nothing

close :: WriteAheadLog a -> IO ()
close wal = withMVar (walHandle wal) $ \(WALHandle h _) -> WALFile.close h

createDirectoryDurably :: OsPath -> IO ()
createDirectoryDurably dir = do
  exists <- doesDirectoryExist dir
  unless exists $ do
    let parent = takeDirectory dir
    when (parent /= dir) (createDirectoryDurably parent)
    createDirectoryIfMissing False dir
    WALFile.syncDirectory parent

-- | Entries in the Log are structured as:
--
-- * 4 bytes for the length of the payload, encoded as a little-endian Word32;
-- * 4 bytes for the checksum (crc32), encoded as a little-endian Word32;
-- * N payload bytes
frame :: BB.Builder -> BB.Builder
frame payloadB =
  let payload = BL.toStrict (BB.toLazyByteString payloadB)
      len = fromIntegral (BS.length payload) :: Word32
      crc = crc32c payload
   in BB.word32LE len <> BB.word32LE crc <> BB.byteString payload

anyWord32le :: Parser Word32
anyWord32le = Parser.take 4 <&> pack . BS.reverse
  where
    pack :: (Bits a, Num a) => StrictByteString -> a
    pack = B.foldl' (\n h -> (n `shiftL` 8) .|. fromIntegral h) 0

-- | Parse the payload of an entry, checking its integrity.
payloadP :: Parser StrictByteString
payloadP = do
  len <- anyWord32le
  checksum <- anyWord32le
  bytes <- Parser.take (fromIntegral len)
  if crc32c bytes /= checksum
    then fail "WAL: checksum mismatch (torn or corrupted write)"
    else pure bytes

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
  bytes <- evaluate (BL.toStrict (BB.toLazyByteString (foldMap (frame . encodeEntry (walCodec wal)) xs)))

  withActiveSegment wal $ \handle -> do
    written <- readIORef (walWritten wal)
    if written >= maxSegmentBytes (walConfig wal)
      then void <$> rotateLocked wal handle
      else pure (handle, ())

  withActiveSegment wal $ \handle@(WALHandle h _) -> do
    durably wal $
      BSU.unsafeUseAsCStringLen bytes $ \(ptr, len) -> do
        WALFile.writeAll h (castPtr ptr) len
        WALFile.flush h
    modifyIORef' (walWritten wal) (+ fromIntegral (BS.length bytes))
    pure (handle, ())

-- | Close the current segment and open a fresh, empty one.
rotate :: (MonadIO m) => WriteAheadLog a -> m Segment
rotate wal = liftIO $ withActiveSegment wal (rotateLocked wal)

-- | Implementation of 'rotate', for when we already have access to
-- the active segment. If rotating fails, the active segment doesn't change.
rotateLocked :: WriteAheadLog a -> WALHandle -> IO (WALHandle, Segment)
rotateLocked wal (WALHandle h _) = do
  durably wal (WALFile.flush h)

  n' <- (+ 1) <$> readIORef (walActiveSegment wal)
  path <- (directory (walConfig wal) </>) <$> segmentPath n'

  -- The next segment file is overwritten
  withBinaryFile path WriteMode (const (pure ()))
  WALFile.syncDirectory (directory (walConfig wal))

  h' <- WALFile.open path
  WALFile.close h

  writeIORef (walActiveSegment wal) n'
  writeIORef (walWritten wal) 0

  pure (WALHandle h' path, n')

-- | Run an action with exclusive access to the active segment.
--
-- Interrupting the action with an asynchronous exception (e.g. from
-- 'System.Timeout.timeout') could leave a partial entry, or a closed
-- handle, behind; such exceptions are deferred until the action completes.
-- Threads waiting for access can still be interrupted.
withActiveSegment :: WriteAheadLog a -> (WALHandle -> IO (WALHandle, b)) -> IO b
withActiveSegment wal = mask_ . modifyMVar (walHandle wal)

-- | Write to the active segment.
-- To prevent data corruption in a multi-threaded context,
-- once a write fails, `durably` will stop working.
durably :: WriteAheadLog a -> IO b -> IO b
durably wal act = do
  failure <- readIORef (walFailure wal)
  forM_ failure $ \e ->
    fail ("WAL: refusing to write, since an earlier write failed (" <> displayException e <> ")")
  act `catch` \e -> do
    writeIORef (walFailure wal) (Just e)
    throwIO (e :: SomeException)

-- | Stream the contents of a write-ahead log segment.
--
-- See 'segments' to fetch available segments for a given 'WriteAheadLog'
--
-- Replaying stops at the first entry which is incomplete or fails its
-- checksum, as happens when a crash interrupts a write. An entry which
-- passes its checksum, but can't be decoded, is an error.
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
      result <- mask $ \restore ->
        modifyMVar (walHandle wal) $ \(WALHandle h fp) -> do
          WALFile.close h
          replayed <- try (restore replayUnusedSegment)
          h' <- WALFile.open fp
          pure (WALHandle h' fp, replayed)
      either throwIO pure (result :: Either SomeException ())
    else replayUnusedSegment
  where
    replayUnusedSegment = do
      p <- segmentPath segment
      withBinaryFile
        (directory (walConfig wal) </> p)
        ReadMode
        $ \h -> streamPayloads h $ \payload ->
          either
            (\e -> fail ("WAL: decode error: " <> e))
            onEntry
            (decodeEntry (walCodec wal) payload)

-- | Replay every segment in the log directory, oldest first, feeding each
-- decoded entry to the given callback in order.
replayAll :: (MonadIO m) => WriteAheadLog a -> (a -> IO ()) -> m ()
replayAll wal onEntry = do
  segs <- listSegmentsFromDir (directory (walConfig wal))
  forM_ segs $ \(seg, _) -> replaySegment wal seg onEntry

-- | Stream the payloads of a segment's entries, in order.
--
-- Streaming stops at the first entry which is incomplete or fails
-- its checksum.
streamPayloads :: Handle -> (StrictByteString -> IO ()) -> IO ()
streamPayloads h onPayload = go (Parser.parse payloadP BS.empty)
  where
    go (Parser.Done rest payload) = onPayload payload >> go (Parser.parse payloadP rest)
    go (Parser.Partial continue) = BS.hGet h (64 * 1024) >>= go . continue
    -- Either we've reached the end of the segment, or the next entry
    -- is torn or corrupted; both mark the end of the valid entries.
    go Parser.Fail {} = pure ()

-- | Returns the size of the data that only includes valid
-- write-ahead log entries.
--
-- This can be used to truncate write-ahead log files to
-- remove partial entries.
validSize :: Handle -> IO Word64
validSize h = do
  validSizeRef <- newIORef (0 :: Word64)

  streamPayloads h $ \payload ->
    modifyIORef' validSizeRef (\v -> v + 4 + 4 + fromIntegral (BS.length payload))
  readIORef validSizeRef
