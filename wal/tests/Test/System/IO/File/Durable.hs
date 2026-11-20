module Test.System.IO.File.Durable (tests) where

import Control.Exception (bracket)
import Control.Monad.IO.Class (liftIO)
import qualified Data.ByteString as BS
import Foreign.Ptr (castPtr)
import Hedgehog (annotate, forAll, property, (===))
import qualified Hedgehog.Gen as Gen
import qualified Hedgehog.Range as Range
import System.Directory.OsPath (doesFileExist, listDirectory)
import System.File.OsPath (readFile')
import System.IO.File.Durable (close, open, openRead, readSome, removeFileDurable, writeAll, writeFileAtomic)
import System.IO.Temp (withSystemTempDirectory)
import System.OsPath (encodeFS, unsafeEncodeUtf, (</>))
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.Hedgehog (testProperty)

tests :: TestTree
tests =
  testGroup
    "System.IO.File.Durable"
    [ testWriteFileAtomic,
      testWriteFileAtomicIgnoresStaleTemporary,
      testRemoveFileDurable,
      testReadWhileWriting
    ]

testWriteFileAtomic :: TestTree
testWriteFileAtomic = testProperty "writeFileAtomic" $ property $ do
  old <- forAll $ Gen.bytes (Range.linear 0 64)
  new <- forAll $ Gen.bytes (Range.linear 0 64)
  (contents, files) <- liftIO $ withSystemTempDirectory "durable-testing" $ \tmpDir -> do
    dir <- encodeFS tmpDir
    let path = dir </> unsafeEncodeUtf "file"
    writeFileAtomic path old
    writeFileAtomic path new
    (,) <$> readFile' path <*> listDirectory dir
  contents === new
  length files === 1

testWriteFileAtomicIgnoresStaleTemporary :: TestTree
testWriteFileAtomicIgnoresStaleTemporary = testProperty "writeFileAtomic discards a temporary file left over by a crash" $ property $ do
  stale <- forAll $ Gen.bytes (Range.linear 1 64)
  new <- forAll $ Gen.bytes (Range.linear 0 64)
  contents <- liftIO $ withSystemTempDirectory "durable-testing" $ \tmpDir -> do
    dir <- encodeFS tmpDir
    let path = dir </> unsafeEncodeUtf "file"
    BS.writeFile (tmpDir <> "/file.tmp") stale
    writeFileAtomic path new
    readFile' path
  annotate "the stale bytes must not be appended to or mixed with the new contents"
  contents === new

testRemoveFileDurable :: TestTree
testRemoveFileDurable = testProperty "removeFileDurable removes a file if it exists" $ property $ do
  bytes <- forAll $ Gen.bytes (Range.linear 0 64)
  (existsBefore, existsAfter) <- liftIO $ withSystemTempDirectory "durable-testing" $ \tmpDir -> do
    dir <- encodeFS tmpDir
    let path = dir </> unsafeEncodeUtf "file"
    removeFileDurable path
    writeFileAtomic path bytes
    before <- doesFileExist path
    removeFileDurable path
    after <- doesFileExist path
    pure (before, after)
  (existsBefore, existsAfter) === (True, False)

testReadWhileWriting :: TestTree
testReadWhileWriting = testProperty "A file can be read while it is open for writing" $ property $ do
  before <- forAll $ Gen.bytes (Range.linear 1 64)
  after <- forAll $ Gen.bytes (Range.linear 1 64)
  seen <- liftIO $ withSystemTempDirectory "durable-testing" $ \tmpDir -> do
    dir <- encodeFS tmpDir
    let path = dir </> unsafeEncodeUtf "file"
        write h bytes = BS.useAsCStringLen bytes $ \(ptr, len) -> writeAll h (castPtr ptr) len
    bracket (open path) close $
      \w -> bracket (openRead path) close $ \r -> do
        write w before
        seenBefore <- readSome r 128
        write w after
        seenAfter <- readSome r 128
        pure (seenBefore, seenAfter)
  seen === (before, after)
