module Main (main) where

import Test.Tasty (testGroup, defaultMain)
import Test.Network.Consensus.Raft (tests)

main :: IO ()
main = defaultMain (testGroup "hs-raft" [tests])
