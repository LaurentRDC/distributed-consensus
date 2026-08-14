{-# LANGUAGE TypeApplications #-}

module Main (main) where

import Data.Proxy (Proxy (..))
import Test.Network.Consensus.Raft (NumRacyTests, PrintTrace, tests)
import Test.Tasty (defaultIngredients, defaultMainWithIngredients, includingOptions, testGroup)
import Test.Tasty.Options (OptionDescription (..))

main :: IO ()
main =
  defaultMainWithIngredients
    ( includingOptions
        [ Option (Proxy @NumRacyTests),
          Option (Proxy @PrintTrace)
        ]
        : defaultIngredients
    )
    (testGroup "hs-raft" [tests])
