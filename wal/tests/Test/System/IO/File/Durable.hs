module Test.System.IO.File.Durable (tests) where

import Control.Monad.IO.Class (liftIO)
import qualified Data.ByteString as BS
import Hedgehog (annotate, forAll, property, (===))
import qualified Hedgehog.Gen as Gen
import qualified Hedgehog.Range as Range
import System.Directory.OsPath (doesFileExist, listDirectory)
import System.File.OsPath (readFile')
import System.IO.File.Durable (removeFileDurable, writeFileAtomic)
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
      testRemoveFileDurable
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
testRemoveFileDurable = testProperty "removeFileDurable removes a file, and is a no-op if it is missing" $ property $ do
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
