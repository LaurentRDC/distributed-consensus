{-# LANGUAGE ScopedTypeVariables #-}

module Test.Distributed.Consensus.Raft.Client
  ( checkLinearizability,

    -- * Client tracing
    RaftClientTrace (..),
  )
where

import Control.Concurrent.Linearizable (Model (..), linearizable, operation, timestampReq)
import qualified Control.Concurrent.Linearizable as Linearizable
import Control.Monad.IOSim (SimEvent, SimEventType (EventLog), SimResult, Trace, selectTraceEvents)
import Data.Dynamic (Typeable, fromDynamic)
import Data.Function ((&))
import Data.List (foldl', sortOn)
import Data.Word (Word64)
import Distributed.Consensus.Raft
  ( runStateMachineStep,
  )
import Test.StateMachine (Command (..), Key, Node (..), Result, State, step)
import Test.Tasty.QuickCheck (Property, counterexample, property, (.&&.))

checkLinearizability :: Trace (SimResult a) SimEvent -> Property
checkLinearizability trace' =
  -- In order to reduce the state space of the linearizability checker,
  -- we check the linearizability of events related to each
  -- individual key independently.
  Data.List.foldl'
    (.&&.)
    (property True)
    -- The scenario inputs are such that there's always at least one
    -- client with at least one command. Therefore, if 'clientEvents' is
    -- null, we've got a problem, since an empty history is trivially linearizable.
    ( counterexample "No client events generated" (not (null clientEvents))
        : (checkLinearizabilityOnKey <$> enumFromTo minBound maxBound)
    )
  where
    (clientEvents :: [RaftClientTrace Command Node Result]) = raftClientTrace trace'

    checkLinearizabilityOnKey :: Key -> Property
    checkLinearizabilityOnKey key =
      let history =
            [ (effectiveNode, event)
            | (ix, MkRaftClientTrace (Node node) event) <- zip [0 :: Int ..] clientEvents,
              commandKey (fst $ operation event) == key,
              -- There's a problem with requests that never get a response.
              -- I consider that they may apply at any time after the request
              -- is sent. However, due to the way 'linearizable' works, the
              -- 'Event' associated with a timed-out request blocks requests
              -- sent after the timed-out one from being re-ordered.
              --
              -- The solution I have here is to let the timed-out request be re-orderable
              -- by associating it with a unique node 'effectiveNode' with only this event.
              -- The use of 'ix' makes this unique.
              --
              -- This concern should be included into 'linearizable' itself
              let effectiveNode = case snd (operation event) of
                    Just _ -> (node, Nothing)
                    Nothing -> (node, Just ix)
            ]
              & sortOn (timestampReq . snd)
       in counterexample
            ("Client history for key " <> show key <> " not linearizable: " <> show history)
            ( linearizable
                model
                history
            )
      where
        commandKey (Insert k _) = k
        commandKey (Delete k) = k
        commandKey (Get k) = k
    -- TODO: extend model to accept events where there was no 'Result' (e.g. timeout),
    --       in which case the state of a particular key is considered 'unknown'.
    --
    --       To do this, 'RaftClientTrace' would have to be extended to represent 'Maybe result'
    model :: Model State (Command, Maybe Result)
    model =
      MkModel
        { initialState = mempty :: State,
          apply = \(command, observedResult) state ->
            let (newState, expectedResult) = runStateMachineStep (Test.StateMachine.step command) state
             in case observedResult of
                  -- Result never received; we assume that it has succeeded and continue
                  Nothing -> Just newState
                  Just result
                    | result == expectedResult -> Just newState
                    | otherwise -> Nothing
        }

raftClientTrace ::
  (Typeable entry, Typeable result, Typeable node) =>
  Trace (SimResult a) SimEvent -> [RaftClientTrace entry node result]
raftClientTrace =
  selectTraceEvents
    ( \_ ev -> case ev of
        EventLog dyn -> fromDynamic dyn
        _ -> Nothing -- internal io-sim event
    )

data RaftClientTrace entry node result
  = MkRaftClientTrace
  { rctNode :: node,
    rctEvent :: Linearizable.Event Word64 (entry, Maybe result)
  }
  deriving (Eq, Show)
