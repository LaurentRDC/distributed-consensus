{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE TupleSections #-}

module Test.System.IO.WAL (tests) where

import Control.Concurrent.Async (async, forConcurrently, wait)
import Control.Exception (SomeException, try)
import Control.Monad (replicateM, unless)
import Control.Monad.IO.Class (liftIO)
import Data.Bits (complement)
import Data.ByteString (StrictByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder as Builder
import qualified Data.ByteString.Char8 as BS8
import Data.Either (isLeft)
import Data.Foldable (for_, toList, traverse_)
import Data.IORef (modifyIORef', newIORef, readIORef)
import Data.Int (Int64)
import Data.List (isPrefixOf, sort)
import Data.List.NonEmpty (nonEmpty)
import qualified Data.List.NonEmpty as NonEmpty
import Data.Traversable (for)
import Hedgehog (Gen, annotate, assert, failure, forAll, property, (===))
import qualified Hedgehog.Gen as Gen
import qualified Hedgehog.Range as Range
import System.File.OsPath (withBinaryFile)
import System.IO (IOMode (..), SeekMode (..), hFileSize, hSeek, hSetFileSize)
import System.IO.Temp (withSystemTempDirectory)
import System.IO.WAL
import System.OsPath (encodeFS, (</>))
import System.Timeout (timeout)
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.Hedgehog (testProperty)

tests :: TestTree
tests =
  testGroup
    "System.IO.WAL"
    [ testTripping,
      testReplayIgnorePartialWrites,
      testOpenRepairsPartialWrites,
      testOpenRepairsCorruptedRecords,
      testReplayFailsOnUndecodableRecords,
      testSingleVsManyAppend,
      testAutomaticRotation,
      testConcurrentAppends,
      testCloseWhileAppending,
      testReplayWhileAppending
    ]

testTripping :: TestTree
testTripping = testProperty "Writing to WAL and replaying" $ property $ do
  replayedRef <- liftIO $ newIORef []
  entriesSegment1 <- forAll $ Gen.list (Range.linear 0 10) (Gen.bytes (Range.linear 0 24))
  entriesSegment2 <- forAll $ Gen.list (Range.linear 0 10) (Gen.bytes (Range.linear 0 24))

  replayed <- liftIO $ withSystemTempDirectory "wal-testing" $ \tmpDir -> do
    dir <- encodeFS tmpDir
    withWriteAheadLog testCodec (WALConfig maxBound dir) $ \wal -> do
      for_ entriesSegment1 (append wal)
      unless (null entriesSegment2) $ do
        _ <- rotate wal
        for_ entriesSegment2 (append wal)

      -- prepending to a list is faster, but then
      -- we need to reverse the output
      replayAll wal (\e -> modifyIORef' replayedRef (e :))
      reverse <$> readIORef replayedRef

  (entriesSegment1 <> entriesSegment2) === replayed

testReplayIgnorePartialWrites :: TestTree
testReplayIgnorePartialWrites = testProperty "Reading from a partially-written WAL surfaces works" $ property $ do
  entries <- forAll $ Gen.list (Range.linear 0 1024) (Gen.bytes (Range.linear 0 107))

  let toFramedSize bytes = 4 + 4 + BS.length bytes
      fullFileSize = sum (toFramedSize <$> entries)

  truncationPoint <- forAll $ Gen.integral (Range.linear 0 fullFileSize)

  let go _ numEntries [] = numEntries
      go lengthSoFar numEntries (e : es) =
        let extended = lengthSoFar + toFramedSize e
         in if extended > truncationPoint
              then numEntries
              else go extended (numEntries + 1) es
      expectedPrefixLength = go 0 0 entries
      expectedPrefix = take expectedPrefixLength entries

  -- sanity check
  annotate $ "Expected prefix: " <> show expectedPrefix
  assert (expectedPrefix `isPrefixOf` entries)

  replayed <- liftIO $ withSystemTempDirectory "wal-testing-partial" $ \tmpDir -> do
    dir <- encodeFS tmpDir
    withWriteAheadLog testCodec (WALConfig maxBound dir) $ \wal -> do
      traverse_ (appendMany wal) (nonEmpty entries)

    fp <- (dir </>) <$> segmentPath 0
    liftIO
      $ withBinaryFile
        fp
        ReadWriteMode
      $ flip hSetFileSize (toInteger truncationPoint)

    replayedRef <- liftIO $ newIORef []
    withWriteAheadLog testCodec (WALConfig maxBound dir) $ \wal -> do
      -- prepending to a list is faster, but then
      -- we need to reverse the output
      replayAll wal (\e -> modifyIORef' replayedRef (e :))
      reverse <$> readIORef replayedRef

  replayed === expectedPrefix

testOpenRepairsPartialWrites :: TestTree
testOpenRepairsPartialWrites = testProperty "Opening a partially-torn WAL results in torn records being truncated" $ property $ do
  entries <- forAll $ Gen.list (Range.linear 0 1024) (Gen.bytes (Range.linear 0 107))

  let toFramedSize bytes = 4 + 4 + BS.length bytes
      fullFileSize = sum (toFramedSize <$> entries)

  truncationPoint <- forAll $ Gen.integral (Range.linear 0 fullFileSize)

  let go _ numEntries [] = numEntries
      go lengthSoFar numEntries (e : es) =
        let extended = lengthSoFar + toFramedSize e
         in if extended > truncationPoint
              then numEntries
              else go extended (numEntries + 1) es
      expectedPrefixLength = go 0 0 entries
      expectedPrefix = take expectedPrefixLength entries

  -- sanity check
  annotate $ "Expected prefix: " <> show expectedPrefix
  assert (expectedPrefix `isPrefixOf` entries)

  size <- liftIO $ withSystemTempDirectory "wal-testing-partial" $ \tmpDir -> do
    dir <- encodeFS tmpDir
    withWriteAheadLog testCodec (WALConfig maxBound dir) $ \wal -> do
      traverse_ (appendMany wal) (nonEmpty entries)

    fp <- (dir </>) <$> segmentPath 0
    liftIO
      $ withBinaryFile
        fp
        ReadWriteMode
      $ flip hSetFileSize (toInteger truncationPoint)

    -- Open the WAL but do nothing
    withWriteAheadLog testCodec (WALConfig maxBound dir) $ \_wal -> pure ()

    liftIO $ withBinaryFile fp ReadMode hFileSize

  fromIntegral size === sum (toFramedSize <$> expectedPrefix)

testOpenRepairsCorruptedRecords :: TestTree
testOpenRepairsCorruptedRecords = testProperty "Opening a WAL with a corrupted record truncates the log at that record" $ property $ do
  entries <- forAll $ Gen.list (Range.linear 1 1024) (Gen.bytes (Range.linear 0 107))
  corrupted <- forAll $ Gen.int (Range.linear 0 (length entries - 1))

  let toFramedSize bytes = 4 + 4 + BS.length bytes
      expectedPrefix = take corrupted entries
      checksumOffset = sum (toFramedSize <$> expectedPrefix) + 4

  (replayed, size) <- liftIO $ withSystemTempDirectory "wal-testing-corrupted" $ \tmpDir -> do
    dir <- encodeFS tmpDir
    withWriteAheadLog testCodec (WALConfig maxBound dir) $ \wal -> do
      traverse_ (appendMany wal) (nonEmpty entries)

    fp <- (dir </>) <$> segmentPath 0
    withBinaryFile fp ReadWriteMode $ \h -> do
      hSeek h AbsoluteSeek (toInteger checksumOffset)
      byte <- BS.hGet h 1
      hSeek h AbsoluteSeek (toInteger checksumOffset)
      BS.hPut h (BS.map complement byte)

    replayed <- timeout 10_000_000 $ withWriteAheadLog testCodec (WALConfig maxBound dir) $ \wal -> do
      replayedRef <- newIORef []
      replayAll wal (\e -> modifyIORef' replayedRef (e :))
      reverse <$> readIORef replayedRef

    (replayed,) <$> withBinaryFile fp ReadMode hFileSize

  replayed === Just expectedPrefix
  fromIntegral size === sum (toFramedSize <$> expectedPrefix)

testReplayFailsOnUndecodableRecords :: TestTree
testReplayFailsOnUndecodableRecords = testProperty "Replaying a record which can't be decoded is an error" $ property $ do
  before <- forAll $ Gen.list (Range.linear 0 100) (Gen.bytes (Range.linear 0 24))
  after <- forAll $ Gen.list (Range.linear 0 100) (Gen.bytes (Range.linear 0 24))

  let undecodable = BS8.pack "undecodable"
      pickyCodec =
        testCodec
          { decodeEntry = \bytes ->
              if bytes == undecodable
                then Left "undecodable entry"
                else Right bytes
          }

  result <- liftIO $ withSystemTempDirectory "wal-testing-undecodable" $ \tmpDir -> do
    dir <- encodeFS tmpDir
    withWriteAheadLog testCodec (WALConfig maxBound dir) $ \wal ->
      for_ (before <> [undecodable] <> after) (append wal)

    timeout 10_000_000 $ withWriteAheadLog pickyCodec (WALConfig maxBound dir) $ \wal ->
      try (replayAll wal (const (pure ())))

  fmap isLeft (result :: Maybe (Either SomeException ())) === Just True

testSingleVsManyAppend :: TestTree
testSingleVsManyAppend = testProperty "append and appendMany give the same result" $ property $ do
  entriesBatched <-
    forAll $
      Gen.list
        (Range.linear 0 100)
        ( Gen.nonEmpty
            (Range.linear 1 3)
            (Gen.bytes (Range.linear 0 24))
        )

  walBound <- forAll $ Gen.int64 (Range.linear 5 200_000_000)

  let entries = foldMap NonEmpty.toList entriesBatched

  replayedSingle <- liftIO $ withSystemTempDirectory "wal-testing" $ \tmpDir -> do
    dir <- encodeFS tmpDir
    withWriteAheadLog testCodec (WALConfig walBound dir) $ \wal -> do
      for_ entries (append wal)

      replayedRef <- liftIO $ newIORef []
      replayAll wal (\e -> modifyIORef' replayedRef (e :))
      reverse <$> readIORef replayedRef

  replayedSingle === entries

  replayedMany <- liftIO $ withSystemTempDirectory "wal-testing" $ \tmpDir -> do
    dir <- encodeFS tmpDir
    withWriteAheadLog testCodec (WALConfig walBound dir) $ \wal -> do
      for_ entriesBatched (appendMany wal)

      replayedRef <- liftIO $ newIORef []
      replayAll wal (\e -> modifyIORef' replayedRef (e :))
      reverse <$> readIORef replayedRef

  replayedMany === entries

testAutomaticRotation :: TestTree
testAutomaticRotation = testProperty "Segments rotate automatically once they are full" $ property $ do
  entries <- forAll $ Gen.list (Range.linear 1 100) (Gen.bytes (Range.linear 0 24))
  maxBytes <- forAll $ Gen.int64 (Range.linear 8 256)

  replayed <- liftIO $ withSystemTempDirectory "wal-testing-rotation" $ \tmpDir -> do
    dir <- encodeFS tmpDir
    timeout 10_000_000 $ withWriteAheadLog testCodec (WALConfig maxBytes dir) $ \wal -> do
      for_ entries (append wal)

      segs <- toList <$> segments wal
      for segs $ \seg -> do
        replayedRef <- newIORef []
        replaySegment wal seg (\e -> modifyIORef' replayedRef (e :))
        reverse <$> readIORef replayedRef

  replayed === Just (expectedSegments maxBytes entries)

expectedSegments :: Int64 -> [StrictByteString] -> [[StrictByteString]]
expectedSegments maxBytes = go 0 []
  where
    go _ current [] = [reverse current]
    go written current (e : es)
      | written >= maxBytes = reverse current : go (framedSize e) [e] es
      | otherwise = go (written + framedSize e) (e : current) es

    framedSize e = fromIntegral (4 + 4 + BS.length e)

testConcurrentAppends :: TestTree
testConcurrentAppends = testProperty "Concurrent appends are all written, in order for each thread" $ property $ do
  entries <- forAll genThreadEntries
  maxBytes <- forAll $ Gen.choice [Gen.int64 (Range.linear 8 256), pure maxBound]

  replayed <- liftIO $ withSystemTempDirectory "wal-testing-concurrent" $ \tmpDir -> do
    dir <- encodeFS tmpDir
    timeout 30_000_000 $ withWriteAheadLog testCodec (WALConfig maxBytes dir) $ \wal -> do
      _ <- forConcurrently entries (traverse_ (append wal))

      segs <- toList <$> segments wal
      for segs $ \seg -> do
        replayedRef <- newIORef []
        replaySegment wal seg (\e -> modifyIORef' replayedRef (e :))
        reverse <$> readIORef replayedRef

  case replayed of
    Nothing -> annotate "Timed out" >> failure
    Just perSegment -> do
      let inLog = concat perSegment
      sort inLog === sort (concat entries)
      for_ entries $ \threadEntries ->
        filter (`elem` threadEntries) inLog === threadEntries
      perSegment === expectedSegments maxBytes inLog

testCloseWhileAppending :: TestTree
testCloseWhileAppending = testProperty "Closing a log while appending to it keeps exactly the successful appends" $ property $ do
  entries <- forAll genThreadEntries

  result <- liftIO $ withSystemTempDirectory "wal-testing-close" $ \tmpDir -> do
    dir <- encodeFS tmpDir
    let config = WALConfig maxBound dir
    timeout 30_000_000 $ do
      (wal, appenders) <- withWriteAheadLog testCodec config $ \wal ->
        (wal,) <$> traverse (async . appendUntilClosed wal) entries
      successes <- traverse wait appenders

      appendAfterClose <- try (append wal (BS8.pack "too late"))

      replayed <- withWriteAheadLog testCodec config $ \wal' -> do
        replayedRef <- newIORef []
        replayAll wal' (\e -> modifyIORef' replayedRef (e :))
        reverse <$> readIORef replayedRef
      pure (successes, replayed, isLeft (appendAfterClose :: Either SomeException ()))

  case result of
    Nothing -> annotate "Timed out" >> failure
    Just (successes, replayed, appendAfterCloseFailed) -> do
      assert appendAfterCloseFailed
      sort replayed === sort (concat successes)
      for_ (zip entries successes) $ \(threadEntries, threadSuccesses) -> do
        assert (threadSuccesses `isPrefixOf` threadEntries)
        filter (`elem` threadEntries) replayed === threadSuccesses
  where
    appendUntilClosed _ [] = pure []
    appendUntilClosed wal (e : es) = do
      appended <- try (append wal e)
      case appended :: Either SomeException () of
        Left _ -> pure []
        Right () -> (e :) <$> appendUntilClosed wal es

testReplayWhileAppending :: TestTree
testReplayWhileAppending = testProperty "Replaying the active segment while appending to it sees a prefix of the log" $ property $ do
  entries <- forAll genThreadEntries
  numReplays <- forAll $ Gen.int (Range.linear 1 5)

  result <- liftIO $ withSystemTempDirectory "wal-testing-replay" $ \tmpDir -> do
    dir <- encodeFS tmpDir
    timeout 30_000_000 $ withWriteAheadLog testCodec (WALConfig maxBound dir) $ \wal -> do
      let replayActive = do
            replayedRef <- newIORef []
            seg <- activeSegment wal
            replaySegment wal seg (\e -> modifyIORef' replayedRef (e :))
            reverse <$> readIORef replayedRef
      snapshots <-
        forConcurrently (Left numReplays : map Right entries) $
          either (`replicateM` replayActive) (\es -> [] <$ traverse_ (append wal) es)
      (concat snapshots,) <$> replayActive

  case result of
    Nothing -> annotate "Timed out" >> failure
    Just (snapshots, final) -> do
      sort final === sort (concat entries)
      for_ snapshots $ \snapshot -> assert (snapshot `isPrefixOf` final)

genThreadEntries :: Gen [[StrictByteString]]
genThreadEntries = do
  paddings <- Gen.list (Range.linear 1 8) (Gen.list (Range.linear 0 20) (Gen.int (Range.linear 0 24)))
  pure
    [ [BS8.pack (show (thread, i)) <> BS.replicate padding 0 | (i, padding) <- zip [0 :: Int ..] threadPaddings]
    | (thread, threadPaddings) <- zip [0 :: Int ..] paddings
    ]

testCodec :: WALCodec StrictByteString
testCodec = WALCodec Builder.byteString pure
