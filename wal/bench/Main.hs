{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE NumericUnderscores #-}

module Main (main) where

import Control.Concurrent.Async (replicateConcurrently_)
import Control.Monad (replicateM)
import Data.Binary (Binary)
import Data.ByteString (StrictByteString)
import qualified Data.ByteString as BS
import qualified Data.List.NonEmpty as NonEmpty
import GHC.Generics (Generic)
import System.IO.Temp (withSystemTempDirectory)
import System.IO.WAL
import System.OsPath (encodeFS)
import Test.Tasty (localOption)
import Test.Tasty.Bench

newtype Entry = Entry StrictByteString
  deriving (Generic)

instance Binary Entry

main :: IO ()
main =
  withSystemTempDirectory "wal-bench" $ \dirFP -> do
    dir <- encodeFS dirFP
    let config =
          WALConfig
            { directory = dir,
              maxSegmentBytes = maxBound
            }

        codec = binaryWALCodec

        entry = Entry (BS.replicate 1024 0x08) -- 1 kB
        batch = flip replicate entry

    withWriteAheadLog codec config $ \wal -> do
      defaultMain . map (localOption WallTime) $
        [ bgroup
            "append"
            [ bench "single" $
                whnfIO (append wal entry)
            ],
          bgroup
            "appendMany"
            [ bench (show batchSize) $
                whnfIO (appendMany wal (NonEmpty.fromList (batch batchSize)))
            | batchSize <- [100, 1000, 10_000]
            ],
          let threadBlocks = [1, 4, 8, 32, 64, 128]
              totalNumberOfWrites = maximum threadBlocks
           in bgroup
                ("concurrent append (" <> show totalNumberOfWrites <> " total writes)")
                [ bench (show numThreads <> " threads") $
                    whnfIO (replicateConcurrently_ numThreads (replicateM numWritesPerThread $ append wal entry))
                | numThreads <- threadBlocks,
                  let numWritesPerThread = totalNumberOfWrites `div` numThreads
                ]
        ]
