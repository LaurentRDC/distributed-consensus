{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module FileSystem
  ( FileSystemCommand (..),
    FileSystemResult (..),
    FileSystem (..),
    FsError (..),

    -- * FileSystem operations
    list,
    touchFile,
    deleteFile,
  )
where

import Data.Binary (Binary)
import Data.ByteString (StrictByteString)
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.String (IsString)
import Data.Text (Text)
import GHC.Generics (Generic)

data FileSystemCommand
  = ListDir
  | TouchFile FileName
  | DeleteFile FileName
  deriving (Eq, Show, Generic)

instance Binary FileSystemCommand

data FileSystemResult
  = FileSystemError FsError
  | FilesFound [FileName]
  | FileTouched FileName
  | FileDeleted FileName
  deriving (Eq, Show, Generic)

instance Binary FileSystemResult

newtype FileSystem = FileSystem
  {fsRoot :: Map FileName StrictByteString}
  deriving (Eq, Show)
  deriving newtype (Binary)

newtype FileName = FileName Text
  deriving stock (Eq, Ord)
  deriving newtype (Binary, Show, IsString)

newtype FsError
  = NoSuchFile FileName
  deriving (Eq, Show, Generic)

instance Binary FsError

list :: FileSystem -> [FileName]
list (FileSystem entries) = Map.keys entries

touchFile :: FileName -> FileSystem -> FileSystem
touchFile name (FileSystem entries) =
  FileSystem $
    Map.insertWith
      (const id) -- preserve the old
      name
      mempty
      entries

deleteFile :: FileName -> FileSystem -> Either FsError FileSystem
deleteFile =
  alter
    (\_ -> Right Nothing)

alter ::
  (StrictByteString -> Either FsError (Maybe StrictByteString)) ->
  FileName ->
  FileSystem ->
  Either FsError FileSystem
alter f name (FileSystem entries) =
  case Map.lookup name entries of
    Nothing -> Left $ NoSuchFile name
    Just file -> case f file of
      Left err -> Left err
      Right modified ->
        Right $ FileSystem $ Map.alter (const modified) name entries
