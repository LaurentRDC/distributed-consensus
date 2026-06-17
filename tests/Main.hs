module Main (main) where

import Test.Network.Consensus.Raft (tests)
import Test.Tasty (defaultMain, testGroup)

main :: IO ()
main = defaultMain (testGroup "hs-raft" [tests])
