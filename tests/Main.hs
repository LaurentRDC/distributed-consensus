{-# LANGUAGE TypeApplications #-}

module Main (main) where

import Data.Proxy (Proxy (..))
import Test.Network.Consensus.Raft (NumRacyTests, tests)
import Test.Tasty (defaultIngredients, defaultMainWithIngredients, includingOptions, testGroup)
import Test.Tasty.Options (OptionDescription (..))

main :: IO ()
main =
  defaultMainWithIngredients
    (includingOptions [Option (Proxy @NumRacyTests)] : defaultIngredients)
    (testGroup "hs-raft" [tests])
