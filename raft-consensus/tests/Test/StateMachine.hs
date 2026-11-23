{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Test.StateMachine
  ( Node (..),
    State,
    Key (..),
    Command (..),
    Result (..),
    step,
  )
where

import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Distributed.Consensus.Raft (StateMachineStep (..))
import Test.Tasty.QuickCheck (Arbitrary (arbitrary), chooseInt, elements, oneof)

-- Simple key-value store

newtype Node = Node Int
  deriving (Eq, Show, Bounded, Enum, Num, Ord, Real, Integral)

type State = Map Key Int

data Command
  = Insert Key Int
  | Delete Key
  | Get Key
  deriving (Eq, Ord, Show)

data Key = A | B | C
  deriving (Eq, Ord, Show, Enum, Bounded)

instance Arbitrary Command where
  arbitrary =
    let possibleKeys = enumFromTo minBound maxBound
     in oneof
          [ Insert <$> elements possibleKeys <*> chooseInt (0, 10),
            Delete <$> elements possibleKeys,
            Get <$> elements possibleKeys
          ]

data Result
  = Value Int
  | Ok
  | Err
  deriving (Eq, Ord, Show)

step :: Command -> StateMachineStep State Result
step (Insert k v) = Transition $ \state -> (Map.insert k v state, Ok)
step (Delete k) = Transition $ \state -> (Map.delete k state, Ok)
step (Get k) = Read $ \state -> maybe Err Value (Map.lookup k state)
