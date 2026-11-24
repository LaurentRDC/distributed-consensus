{-# LANGUAGE TupleSections #-}

module Server.StateMachine
  ( FileSystemCommand (..),
    FileSystemResult (..),
    runFileSystemCommand,
  )
where

import Data.Function ((&))
import Distributed.Consensus.Raft (StateMachineStep (..))
import FileSystem
  ( FileSystem,
    FileSystemCommand (..),
    FileSystemResult (..),
    deleteFile,
    list,
    touchFile,
  )

runFileSystemCommand :: FileSystemCommand -> StateMachineStep FileSystemResult FileSystem
runFileSystemCommand ListDir = Read $ \fs -> list fs & \files -> FilesFound files
runFileSystemCommand (TouchFile path) = Transition $ \fs -> touchFile path fs & (,FileTouched path)
runFileSystemCommand (DeleteFile path) = Transition $ \fs ->
  deleteFile path fs
    & either
      (\err -> (fs, FileSystemError err))
      (,FileDeleted path)
