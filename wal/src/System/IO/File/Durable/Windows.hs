module System.IO.File.Durable.Windows
  ( FHandle,
    open,
    size,
    write,
    flush,
    close,
  )
where

import Data.Word (Word32, Word8)
import Foreign (Ptr)
import System.IO
import System.OsPath (OsPath, decodeFS)
import System.Win32
  ( HANDLE,
    cREATE_ALWAYS,
    closeHandle,
    createFile,
    fILE_ATTRIBUTE_NORMAL,
    fILE_SHARE_NONE,
    flushFileBuffers,
    gENERIC_WRITE,
    win32_WriteFile,
  )
import System.Win32.File (BY_HANDLE_FILE_INFORMATION (bhfiSize), getFileInformationByHandle)

newtype FHandle = FHandle HANDLE

open :: OsPath -> IO FHandle
open filename = do
  fp <- decodeFS filename
  FHandle <$> createFile fp gENERIC_WRITE fILE_SHARE_NONE Nothing cREATE_ALWAYS fILE_ATTRIBUTE_NORMAL Nothing

size :: FHandle -> IO Integer
size (FHandle handle) = fromIntegral . bhfiSize <$> getFileInformationByHandle handle

write :: FHandle -> Ptr Word8 -> Word32 -> IO Word32
write (FHandle handle) data' length = win32_WriteFile handle data' length Nothing

flush :: FHandle -> IO ()
flush (FHandle handle) = flushFileBuffers handle

close :: FHandle -> IO ()
close (FHandle handle) = closeHandle handle
