module Main (main) where

import Test.Control.Concurrent.Linearizable (tests)
import Test.Tasty (defaultMain)

main :: IO ()
main =
  defaultMain tests
