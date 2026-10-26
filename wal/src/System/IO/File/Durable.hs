{-# LANGUAGE CPP #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE QuasiQuotes #-}

-- | Low-level, cross-platform file operations whose effects are
-- guaranteed to have reached stable storage once they return.
module System.IO.File.Durable
  ( -- * Low-level handles
    FHandle,
    open,
    size,
    writeAll,
    flush,
    syncDirectory,
    close,

    -- * Whole-file operations
    writeFileAtomic,
    removeFileDurable,
  )
where

import Control.Exception (bracket)
import Data.ByteString (StrictByteString)
import qualified Data.ByteString as BS
import Data.Word (Word8)
import Foreign.Ptr (Ptr, castPtr, plusPtr)
import System.Directory.OsPath (removeFile, renameFile)
import System.IO.Error (isDoesNotExistError, tryIOError)
import System.OsPath (OsPath, osp, takeDirectory)
import qualified System.OsString as OsString
#ifdef mingw32_HOST_OS
import System.IO.File.Durable.Windows as X
    ( FHandle, open, size, write, flush, syncDirectory, close )
#else
import System.IO.File.Durable.Unix as X
    ( FHandle, open, size, write, flush, syncDirectory, close )
#endif

-- | Write a whole buffer
writeAll :: FHandle -> Ptr Word8 -> Int -> IO ()
writeAll h ptr len
  | len <= 0 = pure ()
  | otherwise = do
      -- write(2) rejects requests larger than INT_MAX bytes on macOS
      written <- fromIntegral <$> write h ptr (fromIntegral (min len (1024 * 1024 * 1024)))
      if written == 0
        then fail "writeAll: no bytes were written"
        else writeAll h (ptr `plusPtr` written) (len - written)

-- | Write durably to a file: once 'writeFileAtomic' returns, the new contents
-- will survive hardware failures, restarts, and crashes.
writeFileAtomic :: OsPath -> StrictByteString -> IO ()
writeFileAtomic path bytes = do
  removeIfExists tmp
  bracket (open tmp) close $ \h -> do
    BS.useAsCStringLen bytes $ \(ptr, len) -> writeAll h (castPtr ptr) len
    flush h
  renameFile tmp path
  syncDirectory (parentDirectory path)
  where
    tmp = path <> [osp|.tmp|]

-- | Remove a file and make the removal durable. If the file does not
-- exist, this function does nothing.
removeFileDurable :: OsPath -> IO ()
removeFileDurable path = do
  removeIfExists path
  syncDirectory (parentDirectory path)

removeIfExists :: OsPath -> IO ()
removeIfExists path =
  tryIOError (removeFile path) >>= \case
    Left e | not (isDoesNotExistError e) -> ioError e
    _ -> pure ()

parentDirectory :: OsPath -> OsPath
parentDirectory path
  | OsString.null dir = [osp|.|]
  | otherwise = dir
  where
    dir = takeDirectory path
