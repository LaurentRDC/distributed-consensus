module Main (main) where

import qualified Test.System.IO.WAL (tests)
import Test.Tasty (defaultMain)

main :: IO ()
main = defaultMain Test.System.IO.WAL.tests
