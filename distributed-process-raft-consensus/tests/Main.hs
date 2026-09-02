module Main (main) where

import qualified Test.Control.Distributed.Process.Raft (tests)
import Test.Tasty (defaultMain)

main :: IO ()
main = defaultMain Test.Control.Distributed.Process.Raft.tests
