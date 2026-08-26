{-# LANGUAGE ForeignFunctionInterface #-}

module System.IO.File.Durable.Unix
  ( FHandle,
    open,
    size,
    write,
    flush,
    close,
  )
where

import Control.Monad (void)
import Data.Word (Word32, Word8)
import Foreign (Ptr)
import Foreign.C (CInt (..))
import System.Posix
  ( Fd (Fd),
    OpenFileFlags (append, creat),
    OpenMode (WriteOnly),
    closeFd,
    defaultFileFlags,
    fdWriteBuf,
    openFd,
    stdFileMode,
  )
import System.Posix.Internals (fdFileSize)

newtype FHandle = FHandle Fd

-- should handle opening flags correctly
open :: FilePath -> IO FHandle
open filename =
  FHandle
    <$> openFd
      filename
      WriteOnly
      defaultFileFlags
        { creat = Just stdFileMode,
          append = True
        }

size :: FHandle -> IO Integer
size (FHandle (Fd h)) = fdFileSize h

write :: FHandle -> Ptr Word8 -> Word32 -> IO Word32
write (FHandle fd) data' length' = fmap fromIntegral $ fdWriteBuf fd data' $ fromIntegral length'

-- Handle error values?
flush :: FHandle -> IO ()
flush (FHandle (Fd c_fd)) = void (c_fsync c_fd)

foreign import ccall "fsync" c_fsync :: CInt -> IO CInt

close :: FHandle -> IO ()
close (FHandle fd) = closeFd fd
