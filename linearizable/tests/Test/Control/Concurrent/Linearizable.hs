{-# LANGUAGE TypeApplications #-}

module Test.Control.Concurrent.Linearizable (tests) where

import Control.Concurrent.Linearizable (Event (..), Model (..), linearizable)
import Data.List.NonEmpty (NonEmpty (..))
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertBool, testCase)

tests :: TestTree
tests =
  testGroup
    "Control.Concurrent.Linearizable"
    [ testEmptyHistoryLinearizable,
      testLinearizableExample,
      testNonLinearizableExample,
      testLinearizableExampleWithFail
    ]

testEmptyHistoryLinearizable :: TestTree
testEmptyHistoryLinearizable =
  testCase "Empty example" $
    let emptyHistory = [] :: [(Int, Event Timestamp Op)]
     in assertBool
          "Trivial history should be linearizable"
          (linearizable model (0 :| [1, 2]) emptyHistory)

testLinearizableExample :: TestTree
testLinearizableExample = testCase "Linearizable example" $ do
  let history =
        [ (2, get Nothing 99 101),
          (0, put 8 100 105),
          (1, put 7 104 106),
          (2, get (Just 7) 102 108),
          (1, get (Just 8) 110 112),
          (2, get (Just 9) 109 115),
          (0, get (Just 8) 111 117),
          (1, put 9 114 118)
        ]
  assertBool "History should be linearizable" (linearizable @Int model (0 :| [1, 2]) history)

testNonLinearizableExample :: TestTree
testNonLinearizableExample = testCase "Non-linearizable example" $ do
  let history =
        [ (0, put 8 100 105),
          (1, put 7 104 106),
          (2, put 9 107 110),
          (0, get (Just 7) 111 113)
        ]
  assertBool "History should not be linearizable" (not $ linearizable @Int model (0 :| [1, 2]) history)

testLinearizableExampleWithFail :: TestTree
testLinearizableExampleWithFail = testCase "Linearizable example with failures" $ do
  let history =
        [ (0, put 8 100 105),
          (1, failed 104 107),
          (1, get (Just 8) 109 110),
          (0, get (Just 7) 111 112),
          (1, get (Just 9) 113 114),
          (1, put 10 115 117),
          (0, get (Just 10) 118 119)
        ]
  assertBool "History should be linearizable" (linearizable @Int model (0 :| [1]) history)

data State
  = Unknown
  | Known (Maybe Int)
  deriving (Eq, Ord)

model :: Model State Op
model =
  MkModel
    { initialState = Known Nothing,
      apply = \op s -> case op of
        Put x -> pure (Known (Just x))
        Get expectation
          | s == Unknown -> pure s
          | s == Known expectation -> pure s
          | otherwise -> Nothing
        Failed -> pure Unknown
    }

type Timestamp = Int

get :: Maybe Int -> Timestamp -> Timestamp -> Event Timestamp Op
get expectation req ack = MkEvent req ack (Get expectation)

put :: Int -> Timestamp -> Timestamp -> Event Timestamp Op
put value req ack = MkEvent req ack (Put value)

failed :: Timestamp -> Timestamp -> Event Timestamp Op
failed req ack = MkEvent req ack Failed

data Op
  = Put Int
  | Get (Maybe Int) -- expected result
  | Failed -- value uncertain
  deriving (Eq, Ord, Show)
