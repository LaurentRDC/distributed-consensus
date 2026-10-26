{-# LANGUAGE CApiFFI #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE ForeignFunctionInterface #-}

module System.IO.File.Durable.Unix
  ( FHandle,
    open,
    size,
    write,
    flush,
    syncDirectory,
    close,
  )
where

#if defined(darwin_HOST_OS)
import Control.Monad (when)
#endif
import Control.Exception (bracket)
import Data.Word (Word32, Word8)
import Foreign (Ptr)
import Foreign.C (CInt (..), throwErrnoIfMinus1Retry_)
#if defined(darwin_HOST_OS)
import Foreign.C (eINTR, eNOTSUP, getErrno, throwErrno)
#endif
import System.OsPath (OsPath)
import System.OsString.Internal.Types (getOsString)
import System.Posix
  ( Fd (Fd),
    OpenFileFlags (append, creat),
    OpenMode (ReadOnly, WriteOnly),
    closeFd,
    defaultFileFlags,
    fdWriteBuf,
    stdFileMode,
  )
import System.Posix.IO.PosixString (openFd)
import System.Posix.Internals (fdFileSize)

newtype FHandle = FHandle Fd

-- should handle opening flags correctly
open :: OsPath -> IO FHandle
open filename =
  FHandle
    <$> openFd
      (getOsString filename)
      WriteOnly
      defaultFileFlags
        { creat = Just stdFileMode,
          append = True
        }

size :: FHandle -> IO Integer
size (FHandle (Fd h)) = fdFileSize h

write :: FHandle -> Ptr Word8 -> Word32 -> IO Word32
write (FHandle fd) data' length' = fmap fromIntegral $ fdWriteBuf fd data' $ fromIntegral length'

-- | Block until everything written to the file has reached stable storage.
flush :: FHandle -> IO ()
flush (FHandle (Fd c_fd)) = fullSync c_fd

-- | Block until the entries of a directory (e.g. newly-created files) have
-- been persisted.
syncDirectory :: OsPath -> IO ()
syncDirectory dirname =
  bracket (openFd (getOsString dirname) ReadOnly defaultFileFlags) closeFd $
    \(Fd c_fd) -> fullSync c_fd

fullSync :: CInt -> IO ()
#if defined(darwin_HOST_OS)
-- On Mac OS, fsync only hands data over to the drive, which may keep it in a
-- volatile cache. F_FULLFSYNC also asks the drive to flush that cache.
-- Some filesystems (e.g. SMB) don't support F_FULLFSYNC, in which case we
-- fall back to fsync, like the Go standard library does.
fullSync c_fd = do
  result <- c_fcntl c_fd c_F_FULLFSYNC
  when (result == -1) $ do
    errno <- getErrno
    if errno == eINTR
      then fullSync c_fd
      else
        if errno == eNOTSUP
          then throwErrnoIfMinus1Retry_ "fsync" (c_fsync c_fd)
          else throwErrno "fcntl(F_FULLFSYNC)"

foreign import capi safe "fcntl.h fcntl" c_fcntl :: CInt -> CInt -> IO CInt

foreign import capi "fcntl.h value F_FULLFSYNC" c_F_FULLFSYNC :: CInt
#else
fullSync c_fd = throwErrnoIfMinus1Retry_ "fsync" (c_fsync c_fd)
#endif

foreign import ccall "fsync" c_fsync :: CInt -> IO CInt

close :: FHandle -> IO ()
close (FHandle fd) = closeFd fd
