-----------------------------------------------------------------------------

-- |
-- Module      :  $module
-- Portability :  portable
--
-- Simple linearizability checker based on
--
--   * M. P. Herlihy and J. M. Wing, \"/Linearizability: a correctness condition for concurrent objects/\",
--     ACM Transactions on Programming Languages and Systems. Volume 12, issue 3, pages 463 - 492.
--
-- as vulgarized by Guanzhou Hu (https://www.josehu.com/technical/2024/07/28/on-line-linearizability-checker.html)
module Control.Concurrent.Linearizable
  ( Model (..),
    Event (..),
    linearizable,
  )
where

import Data.Foldable (foldlM)
import Data.List.NonEmpty (NonEmpty)
import qualified Data.List.NonEmpty as NonEmpty
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.Maybe (mapMaybe)
import Data.Sequence (Seq, ViewL ((:<)), (|>))
import qualified Data.Sequence as Seq
import Data.Set (Set)
import qualified Data.Set as Set

data Event timestamp operation
  = MkEvent
  { timestampReq :: timestamp,
    timestampAck :: timestamp,
    operation :: operation
  }
  deriving (Eq, Ord)

data Model state operation
  = MkModel
  { initialState :: state,
    apply :: operation -> state -> Maybe state
  }

data Possibility node state timestamp operation
  = MkPossibility
  { state :: state,
    queues :: Map node (Seq (Event timestamp operation))
  }
  deriving (Eq, Ord)

data Checker node state timestamp operation
  = MkChecker
  { model :: Model state operation,
    possibilities :: Set (Possibility node state timestamp operation)
  }

-- | Check a model for a state machine against an experimental
-- history of events.
--
-- Returns 'True' if the
linearizable ::
  (Ord node, Ord state, Ord timestamp, Ord operation) =>
  Model state operation ->
  NonEmpty node ->
  [(node, Event timestamp operation)] ->
  Bool
linearizable m nodes events =
  maybe
    False
    atLeastOneLinearHistory
    (foldlM feed (checker nodes) events)
  where
    atLeastOneLinearHistory c =
      any (all null . queues) $
        stepAll
          Final
          (model c)
          (possibilities c)

    checker ns =
      MkChecker m $
        Set.singleton
          MkPossibility
            { state = initialState m,
              queues =
                Map.fromList
                  [ (n, mempty)
                  | n <- NonEmpty.toList ns
                  ]
            }

feed ::
  (Ord node, Ord state, Ord timestamp, Ord operation) =>
  Checker node state timestamp operation ->
  (node, Event timestamp operation) ->
  Maybe (Checker node state timestamp operation)
feed c (node, event)
  | Set.null newPossibilities = Nothing
  | otherwise = Just c {possibilities = newPossibilities}
  where
    newPossibilities =
      let enqueue p = p {queues = Map.adjust (|> event) node (queues p)}
       in stepAll Online (model c) (Set.map enqueue (possibilities c))

data SteppingMode
  = Online
  | Final

stepAll ::
  (Ord state, Ord node, Ord timestamp, Ord operation) =>
  SteppingMode ->
  Model state operation ->
  Set (Possibility node state timestamp operation) ->
  Set (Possibility node state timestamp operation)
stepAll mode m ps
  | Set.null ready = ps
  | otherwise = stepAll mode m (rest `Set.union` Set.fromList (concatMap successors (Set.toList ready)))
  where
    (ready, rest) =
      let steppable p = case mode of
            Online -> not (any null (queues p))
            Final -> not (all null (queues p))
       in Set.partition steppable ps

    successors p = mapMaybe step (Map.toList heads)
      where
        safeHead xs = case Seq.viewl xs of
          Seq.EmptyL -> Nothing
          (x :< _) -> Just x

        heads = Map.mapMaybe safeHead (queues p)

        minAck =
          -- using minimum here is safe because our checker takes 'NonEmpty node' as input,
          -- hence there is always at least one head
          minimum (timestampAck <$> heads)

        step (n, h)
          | timestampReq h < minAck = do
              v <- apply m (operation h) (state p)
              pure $ p {state = v, queues = Map.adjust (Seq.drop 1) n (queues p)}
          | otherwise = Nothing
