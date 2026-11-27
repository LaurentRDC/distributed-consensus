-----------------------------------------------------------------------------

-- |
-- Module:      $module
-- Copyright:   (c) Laurent P. René de Cotret
-- License:     BSD 3-Clause
--
-- Simple linearizability checker based on
--
--   * M. P. Herlihy and J. M. Wing, \"/Linearizability: a correctness condition for concurrent objects/\",
--     ACM Transactions on Programming Languages and Systems. Volume 12, issue 3, pages 463 - 492.
--
-- as <https://www.josehu.com/technical/2024/07/28/on-line-linearizability-checker.html vulgarized by Guanzhou Hu>.
--
-- === How to use this module
--
-- The only useful function here is 'linearizable'. The idea is that 'linearizable' checks
-- whether it is /possible/ that the events fed to it were ever part of a linear history.
module Control.Concurrent.Linearizable
  ( linearizable,

    -- * Modeling
    Model (..),
    Event (..),
  )
where

import Data.Foldable (foldlM)
import Data.List (sortOn)
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.Maybe (mapMaybe)
import Data.Sequence (Seq, ViewL ((:<)), (|>))
import qualified Data.Sequence as Seq
import Data.Set (Set)
import qualified Data.Set as Set

-- | Experimental event. Note that, typically, the @operation@ type
-- parameter tracks both what was the input to the state machine,
-- as well as what was the result of the input.
data Event timestamp operation
  = MkEvent
  { timestampReq :: timestamp,
    timestampAck :: timestamp,
    operation :: operation
  }
  deriving (Show, Eq, Ord)

-- | Model for a state machine.
--
-- Consider a state machine that is a single counter. The 'Model' would be:
--
-- @
-- data Command
--     = Increment
--     | Decrement
--
-- model :: Model Int (Command, Int)
-- model = Model
--       { initialState = 0
--       , apply = \\(op, result) value ->
--                   let expectedState = case op of
--                          Increment = succ value
--                          Decrement = pred value
--                   in if result == expectation
--                          then Just expectation
--                          else Nothing
--       }
-- @
--
-- Here, the @operation@ type parameter represents both what the input
-- was (here, @Command@), as well as what the experimental result of that
-- command was (here, @Int@, the new state of the counter).
--
-- In particular, note how the model returns @'Nothing'@ in case the state is
-- inconsistent with the experimental history.
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
-- history of events. Events will be sorted appropriately before
-- linearizability is checked.
--
-- Returns 'True' if it's possible to place events in linear order
-- that is accepted by the 'Model'.
--
-- Note that checking linearizbility is NP-complete; this means that
-- 'linearizable' may take a long time, and/or require lots of resources.
-- If you can, try to break down the 'Model' into individual components.
-- For example, if you were testing the linearizability of operations
-- on a key-value store, you might want to check the
-- linearizability of events related to each key individually.
linearizable ::
  ( Ord node,
    Ord state,
    Ord timestamp,
    Ord operation
  ) =>
  Model state operation ->
  -- | Experimental history. The order of events need not be sorted in advance.
  [(node, Event timestamp operation)] ->
  Bool
linearizable _ [] = True
linearizable m events =
  maybe
    False
    (any (canDrain m) . possibilities)
    (foldlM feed (checker nodes) (zip sortedEvents stillActive))
  where
    nodes = foldMap (Set.singleton . fst) events

    sortedEvents = sortOn (timestampAck . snd) events
    -- stillActive !! i is the set of nodes that have at least one event
    -- /after/ the i-th event, i.e. the nodes we must still wait for
    stillActive = drop 1 (scanr (Set.insert . fst) Set.empty sortedEvents)

    checker ns =
      MkChecker m $
        Set.singleton
          MkPossibility
            { state = initialState m,
              queues =
                Map.fromList
                  [ (n, mempty)
                  | n <- Set.toList ns
                  ]
            }

feed ::
  (Ord node, Ord state, Ord timestamp, Ord operation) =>
  Checker node state timestamp operation ->
  ((node, Event timestamp operation), Set node) ->
  Maybe (Checker node state timestamp operation)
feed c ((node, event), active)
  | Set.null newPossibilities = Nothing
  | otherwise = Just c {possibilities = newPossibilities}
  where
    newPossibilities =
      let enqueue p = p {queues = Map.adjust (|> event) node (queues p)}
       in stepAll active (model c) (Set.map enqueue (possibilities c))

stepAll ::
  (Ord state, Ord node, Ord timestamp, Ord operation) =>
  -- | Nodes that may still produce events
  Set node ->
  Model state operation ->
  Set (Possibility node state timestamp operation) ->
  Set (Possibility node state timestamp operation)
stepAll active m ps
  | Set.null ready = ps
  | otherwise = stepAll active m (rest `Set.union` Set.fromList (concatMap (successors m) (Set.toList ready)))
  where
    (ready, rest) = Set.partition steppable ps

    steppable p =
      not (all null (queues p))
        && and [not (null q) | (n, q) <- Map.toList (queues p), n `Set.member` active]

successors ::
  (Ord node, Ord timestamp) =>
  Model state operation ->
  Possibility node state timestamp operation ->
  [Possibility node state timestamp operation]
successors m p
  | Map.null heads = []
  | otherwise = mapMaybe step (Map.toList heads)
  where
    safeHead xs = case Seq.viewl xs of
      Seq.EmptyL -> Nothing
      (x :< _) -> Just x

    heads = Map.mapMaybe safeHead (queues p)

    minAck = minimum (timestampAck <$> heads)

    step (n, h)
      | timestampReq h < minAck = do
          v <- apply m (operation h) (state p)
          pure $ p {state = v, queues = Map.adjust (Seq.drop 1) n (queues p)}
      | otherwise = Nothing

-- | Once the whole history has been read, search depth-first for /one/ way
-- to linearize all of the remaining events.
canDrain ::
  (Ord node, Ord state, Ord timestamp) =>
  Model state operation ->
  Possibility node state timestamp operation ->
  Bool
canDrain m p0 = fst (go Set.empty p0)
  where
    key p = (state p, Seq.length <$> Map.elems (queues p))

    go seen p
      | all null (queues p) = (True, seen)
      | key p `Set.member` seen = (False, seen)
      | otherwise = firstSuccess (Set.insert (key p) seen) (successors m p)

    firstSuccess seen [] = (False, seen)
    firstSuccess seen (q : qs) = case go seen q of
      (True, seen') -> (True, seen')
      (False, seen') -> firstSuccess seen' qs
