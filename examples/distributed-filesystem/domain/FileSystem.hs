{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}

module FileSystem
  ( FileSystemCommand (..),
    FileSystemResult (..),
    FileSystem (..),
    Directory (..),
    Path,
    mkPath,
    FsError (..),

    -- * FileSystem operations

    -- ** Create
    touchFile,
    mkDir,

    -- ** Read

    -- ** Update

    -- ** Delete
    deleteFile,
    deleteDir,
  )
where

import Data.Binary (Binary)
import Data.ByteString (StrictByteString)
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.String (IsString)
import Data.Text (Text)
import qualified Data.Text as Text
import GHC.Generics (Generic)

data FileSystemCommand
  = TouchFile Path
  | MkDir Path
  | DeleteFile Path
  | DeleteDir Path
  deriving (Eq, Show, Generic)

instance Binary FileSystemCommand

data FileSystemResult
  = FileSystemError FsError
  | FileTouched Path
  | DirCreated Path
  | FileDeleted Path
  | DirDeleted Path
  deriving (Eq, Show, Generic)

instance Binary FileSystemResult

newtype FileSystem = FileSystem
  {fsRoot :: Directory}
  deriving (Eq, Show)
  deriving newtype (Binary)

data Content
  = File StrictByteString
  | Dir Directory
  deriving (Eq, Show, Generic)

emptyFile :: Content
emptyFile = File mempty

emptyDir :: Content
emptyDir = Dir (Directory mempty)

instance Binary Content

newtype Directory = Directory (Map FileName Content)
  deriving stock (Eq, Show)
  deriving newtype (Binary)

data Path = Path
  { fpDirSegments :: [FileName],
    fpName :: !FileName
  }
  deriving (Eq, Show, Generic)

sep :: Char
sep = '/'

mkPath :: Text -> Path
mkPath p = case Text.split (== sep) p of
  [] -> Path [] (FileName mempty)
  segments -> Path (FileName <$> init segments) (FileName $ last segments)

instance Binary Path

newtype FileName = FileName Text
  deriving stock (Eq, Show, Ord)
  deriving newtype (Binary, IsString)

data FsError
  = CannotModifyRoot
  | NoSuchFileOrDirectory FileName
  | DirAlreadyExists FileName
  | NotADirectory FileName
  | NotAFile FileName
  deriving (Eq, Show, Generic)

instance Binary FsError

touchFile :: Path -> FileSystem -> Either FsError FileSystem
touchFile = alter (maybe (Right (Just emptyFile)) (Right . Just))

mkDir :: Path -> FileSystem -> Either FsError FileSystem
mkDir path =
  alter
    ( maybe
        (Right (Just emptyDir))
        (\_ -> Left (DirAlreadyExists (fpName path)))
    )
    path

deleteFile :: Path -> FileSystem -> Either FsError FileSystem
deleteFile path =
  alter
    ( maybe
        (Left (NoSuchFileOrDirectory (fpName path)))
        ( \case
            File _ -> Right Nothing
            Dir _ -> Left (NotAFile (fpName path))
        )
    )
    path

deleteDir :: Path -> FileSystem -> Either FsError FileSystem
deleteDir path =
  alter
    ( maybe
        (Left (NoSuchFileOrDirectory (fpName path)))
        ( \case
            Dir _ -> Right Nothing
            File _ -> Left (NotAFile (fpName path))
        )
    )
    path

alter ::
  --
  (Maybe Content -> Either FsError (Maybe Content)) ->
  Path ->
  FileSystem ->
  Either FsError FileSystem
alter f (Path path name) (FileSystem root) =
  FileSystem <$> go path root
  where
    go [] (Directory entries) = do
      entry <- f (Map.lookup name entries)
      pure $
        Directory (Map.alter (const entry) name entries)
    go (segment : rest) (Directory entries) = do
      entry <- case Map.lookup segment entries of
        Nothing ->
          Left (NoSuchFileOrDirectory segment)
        Just entry ->
          Right entry

      dir <- case entry of
        File _ ->
          Left (NotADirectory segment)
        Dir dir ->
          Right dir

      dir' <- go rest dir

      pure $
        Directory (Map.insert segment (Dir dir') entries)
