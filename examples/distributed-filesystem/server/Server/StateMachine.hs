{-# LANGUAGE TupleSections #-}

module Server.StateMachine
  ( FileSystemCommand (..),
    FileSystemResult (..),
    runFileSystemCommand,
  )
where

import Data.Function ((&))
import FileSystem
  ( FileSystem,
    FileSystemCommand (..),
    FileSystemResult (..),
    deleteFile,
    mkDir,
    touchFile,
  )

runFileSystemCommand :: FileSystem -> FileSystemCommand -> (FileSystem, FileSystemResult)
runFileSystemCommand fs (TouchFile path) =
  touchFile path fs
    & either
      (\err -> (fs, FileSystemError err))
      (,FileTouched path)
runFileSystemCommand fs (MkDir path) =
  mkDir path fs
    & either
      (\err -> (fs, FileSystemError err))
      (,DirCreated path)
runFileSystemCommand fs (DeleteFile path) =
  deleteFile path fs
    & either
      (\err -> (fs, FileSystemError err))
      (,FileDeleted path)
runFileSystemCommand fs (DeleteDir path) =
  deleteFile path fs
    & either
      (\err -> (fs, FileSystemError err))
      (,DirDeleted path)
