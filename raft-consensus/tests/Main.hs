{-# LANGUAGE TypeApplications #-}

module Main (main) where

import Data.Proxy (Proxy (..))
import Test.Distributed.Consensus.Raft (tests)
import Test.Distributed.Consensus.Raft.Options (BranchingFactor, NumRacyTests, PrintTrace, ScheduleBound)
import Test.Tasty (defaultIngredients, defaultMainWithIngredients, includingOptions, testGroup)
import Test.Tasty.Options (OptionDescription (..))

main :: IO ()
main =
  defaultMainWithIngredients
    ( includingOptions
        [ Option (Proxy @NumRacyTests),
          Option (Proxy @PrintTrace),
          Option (Proxy @ScheduleBound),
          Option (Proxy @BranchingFactor)
        ]
        : defaultIngredients
    )
    (testGroup "raft-consensus" [tests])
