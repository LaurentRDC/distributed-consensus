{-# LANGUAGE CPP #-}

module System.IO.File.Durable
  ( FHandle,
    open,
    size,
    write,
    flush,
    close,
  )
where

#ifdef mingw32_HOST_OS
import System.IO.File.Durable.Windows as X
    ( FHandle, open, size, write, flush, close )
#else
import System.IO.File.Durable.Unix as X
    ( FHandle, open, size, write, flush, close )
#endif
