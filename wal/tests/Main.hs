module Main (main) where

import qualified Test.System.IO.File.Durable (tests)
import qualified Test.System.IO.WAL (tests)
import Test.Tasty (defaultMain, localOption, testGroup)
import Test.Tasty.Hedgehog (HedgehogTestLimit (..))

main :: IO ()
main =
  defaultMain $
    localOption (HedgehogTestLimit (Just 25)) $ -- durable file operations are SLOW
      testGroup "wal" [Test.System.IO.File.Durable.tests, Test.System.IO.WAL.tests]
