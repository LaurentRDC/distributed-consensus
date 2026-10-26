{-# LANGUAGE CPP #-}

module System.IO.File.Durable
  ( FHandle,
    open,
    size,
    writeAll,
    flush,
    syncDirectory,
    close,
  )
where

import Data.Word (Word8)
import Foreign.Ptr (Ptr, plusPtr)
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
