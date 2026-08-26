{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE NumericUnderscores #-}

module Main (main) where

import Data.Binary (Binary)
import Data.ByteString (StrictByteString)
import qualified Data.ByteString as BS
import qualified Data.List.NonEmpty as NonEmpty
import GHC.Generics (Generic)
import System.IO.Temp (withSystemTempDirectory)
import System.IO.WAL
import Test.Tasty.Bench

newtype Entry = Entry StrictByteString
  deriving (Generic)

instance Binary Entry

main :: IO ()
main =
  withSystemTempDirectory "wal-bench" $ \dir -> do
    let config =
          WALConfig
            { directory = dir,
              maxSegmentBytes = maxBound
            }

        codec = binaryWALCodec

        entry = Entry (BS.replicate 1024 0x08) -- 1 kB
        batch = flip replicate entry

    withWriteAheadLog codec config $ \wal -> do
      defaultMain
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
            ]
        ]
