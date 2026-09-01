{-# LANGUAGE NumericUnderscores #-}

module Test.System.IO.WAL (tests) where

import Control.Monad (unless)
import Control.Monad.IO.Class (liftIO)
import Data.ByteString (StrictByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder as Builder
import Data.Foldable (for_, traverse_)
import Data.IORef (modifyIORef', newIORef, readIORef)
import Data.List (isPrefixOf)
import Data.List.NonEmpty (nonEmpty)
import qualified Data.List.NonEmpty as NonEmpty
import Hedgehog (annotate, assert, forAll, property, (===))
import qualified Hedgehog.Gen as Gen
import qualified Hedgehog.Range as Range
import System.File.OsPath (withBinaryFile)
import System.IO (IOMode (..), hFileSize, hSetFileSize)
import System.IO.Temp (withSystemTempDirectory)
import System.IO.WAL
import System.OsPath (encodeFS, (</>))
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.Hedgehog (testProperty)

tests :: TestTree
tests =
  testGroup
    "System.IO.WAL"
    [ testTripping,
      testReplayIgnorePartialWrites,
      testOpenRepairsPartialWrites,
      testSingleVsManyAppend
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

testCodec :: WALCodec StrictByteString
testCodec = WALCodec Builder.byteString pure
