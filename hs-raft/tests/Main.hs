{-# LANGUAGE TypeApplications #-}

module Main (main) where

import Data.Proxy (Proxy (..))
import Test.Network.Consensus.Raft (BranchingFactor, NumRacyTests, PrintTrace, ScheduleBound, tests)
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
    (testGroup "hs-raft" [tests])
