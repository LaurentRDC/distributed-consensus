module System.IO.File.Durable.Windows
  ( FHandle,
    open,
    size,
    write,
    flush,
    syncDirectory,
    close,
  )
where

import Control.Exception (onException)
import Data.Word (Word32, Word8)
import Foreign (Ptr)
import System.IO
import System.OsPath (OsPath)
import System.OsString.Internal.Types (getOsString)
import System.Win32
  ( HANDLE,
    closeHandle,
    fILE_ATTRIBUTE_NORMAL,
    fILE_END,
    fILE_SHARE_NONE,
    flushFileBuffers,
    gENERIC_WRITE,
    oPEN_ALWAYS,
    setFilePointerEx,
    win32_WriteFile,
  )
import System.Win32.File (BY_HANDLE_FILE_INFORMATION (bhfiSize), getFileInformationByHandle)
import System.Win32.WindowsString.File (createFile)

newtype FHandle = FHandle HANDLE

open :: OsPath -> IO FHandle
open filename = do
  h <- createFile (getOsString filename) gENERIC_WRITE fILE_SHARE_NONE Nothing oPEN_ALWAYS fILE_ATTRIBUTE_NORMAL Nothing
  _ <- setFilePointerEx h 0 fILE_END `onException` closeHandle h
  pure (FHandle h)

size :: FHandle -> IO Integer
size (FHandle handle) = fromIntegral . bhfiSize <$> getFileInformationByHandle handle

write :: FHandle -> Ptr Word8 -> Word32 -> IO Word32
write (FHandle handle) data' length = win32_WriteFile handle data' length Nothing

flush :: FHandle -> IO ()
flush (FHandle handle) = flushFileBuffers handle

-- Windows has no equivalent to flushing a directory
syncDirectory :: OsPath -> IO ()
syncDirectory _ = pure ()

close :: FHandle -> IO ()
close (FHandle handle) = closeHandle handle
