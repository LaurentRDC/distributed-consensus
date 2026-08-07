{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module Test.Network.Consensus.Raft (tests) where

import Control.Concurrent.Class.MonadSTM (atomically, modifyTVar', newTVarIO, readTVar, retry, writeTVar)
import Control.Monad (when)
import Control.Monad.Class.MonadAsync (forConcurrently_, withAsync)
import Control.Monad.Class.MonadTimer (threadDelay)
import Control.Monad.IOSim (IOSim, exploreSimTrace, traceM)
import qualified Data.Foldable as Foldable
import Data.Functor ((<&>))
import Data.IntMap (IntMap)
import qualified Data.IntMap.Strict as IntMap
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.Sequence (Seq (..))
import qualified Data.Sequence as Seq
import Data.Set (Set)
import qualified Data.Set as Set
import Network.Consensus.Raft
  ( Config (..),
    Microseconds (Microseconds),
    RaftSpec (..),
    runRaftT,
    server,
  )
import Network.Consensus.Raft.Client (RaftClientSpec (..), RaftClientT, request, runRaftClientT)
import Test.Network.Consensus.Raft.Properties (allProperties)
import Test.Network.Consensus.Scenario (checkScenario)
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.QuickCheck

tests :: TestTree
tests =
  testGroup
    "Raft"
    [ testGroup
        "Property tests"
        [ testCluster
        ]
    ]

testCluster :: TestTree
testCluster =
  testProperty "Cluster properties" $
    property $
      forAll (elements [1 .. 5]) $ \clusterSize ->
        -- The following timings ensure that once election happens,
        -- the heartbeat will be sent early enough to ensure the leader
        -- remains a leader.
        forAll (vectorOf clusterSize (chooseBoundedIntegral (0, 1_000_000))) $ \seeds ->
          forAll (chooseBoundedIntegral (1_000, 200_000)) $ \heartbeatTimeout ->
            -- Making the lower election timeout possibly shorter than the heartbeat timeout
            -- allows to have terms with no leaders elected
            forAll (chooseBoundedIntegral (round $ (0.9 :: Double) * fromIntegral heartbeatTimeout, heartbeatTimeout * 10)) $ \electionTimeoutLowerBound ->
              forAll (chooseBoundedIntegral (electionTimeoutLowerBound, 2 * electionTimeoutLowerBound)) $ \electionTimeoutUpperBound ->
                -- Number of commands tuned for the test suite to take a few seconds.
                forAll (vectorOf 30 (arbitrary @Command)) $ \commands ->
                  counterexample
                    ( unlines
                        [ "cluster size =" <> show clusterSize,
                          "seeds = " <> show seeds,
                          "heartbeat timeout = " <> show heartbeatTimeout,
                          "election timeout lower bound = " <> show electionTimeoutLowerBound,
                          "election timeout upper bound = " <> show electionTimeoutUpperBound
                        ]
                    )
                    $ exploreSimTrace
                      id
                      (scenario heartbeatTimeout electionTimeoutLowerBound electionTimeoutUpperBound (Set.fromList seeds) commands)
                      ( \_ trace ->
                          checkScenario
                            (allProperties @Command @Result @Node)
                            trace
                      )
  where
    mkConfigs heartbeatTimeout electionTimeoutLowerBound electionTimeoutUpperBound seeds =
      let nodes = Set.fromList [0 .. length seeds - 1]
       in IntMap.fromList
            [ ( ix,
                MkConfig
                  { nodeId = ix,
                    otherNodes = nodes `Set.difference` Set.singleton ix,
                    electionTimeoutRange = (Microseconds electionTimeoutLowerBound, Microseconds electionTimeoutUpperBound),
                    heartBeatTimeout = Microseconds heartbeatTimeout,
                    randomSeed = seed
                  }
              )
            | (ix, seed) <- zip [0 ..] (Set.toList seeds)
            ]

    runServers configs specs =
      forConcurrently_ (IntMap.toList specs) $ \(ix, spec) ->
        runRaftT (configs IntMap.! ix) mempty spec server

    runClient _ [] = pure ()
    runClient runRequest ((command, state, expectedResult) : rest) = do
      threadDelay 10_000
      runRequest (request 0 command) >>= \case
        -- Command needs to be re-tried
        Left _ -> runClient runRequest ((command, state, expectedResult) : rest)
        Right actualResult -> do
          when (actualResult /= expectedResult) (fail "Unexpected state")
          runClient runRequest rest

    scenario heartbeatTimeout electionTimeoutLowerBound electionTimeoutUpperBound seeds commands = do
      let configs = mkConfigs heartbeatTimeout electionTimeoutLowerBound electionTimeoutUpperBound seeds
          stateMachineExpectations = expectedResults commands
      MkHarness serverSpecs clientSpec <- testHarness (Set.fromList $ IntMap.keys configs)
      -- Run servers until the clients are done interacting
      withAsync (runServers configs serverSpecs) $ \_ ->
        runClient clientSpec stateMachineExpectations

data Harness s
  = MkHarness
  { serverSpecifications :: IntMap (RaftSpec Command Node State Result (IOSim s)),
    -- TODO: have multiple concurrent clients
    runClientAction :: forall a. RaftClientT Command Node Result (IOSim s) a -> IOSim s a
  }

testHarness ::
  Set Node ->
  IOSim s (Harness s)
testHarness serverNodes = do
  rpcMailbox <- newTVarIO mempty
  rpcResultsMailbox <- newTVarIO mempty
  requestsMailbox <- newTVarIO mempty
  responsesMailbox <- newTVarIO mempty
  pure $
    MkHarness
      { serverSpecifications = IntMap.fromList [(node, serverSpec rpcMailbox rpcResultsMailbox requestsMailbox responsesMailbox node) | node <- Set.toList serverNodes],
        runClientAction = \action -> runRaftClientT action clientNode (clientSpec requestsMailbox responsesMailbox)
      }
  where
    clientNode = maybe 0 succ (Set.lookupMax serverNodes)

    serverSpec rpcMailbox rpcResultsMailbox requestsMailbox responsesMailbox node =
      MkRaftSpec
        { _readLogEntry = \_ -> pure Nothing,
          _writeLogEntry = \_ _ _ -> pure (),
          _readTerm = pure 0,
          _writeTerm = \_ -> pure (),
          _readVotedFor = pure Nothing,
          _voteFor = \_ -> pure (),
          _applyLogEntry = step,
          _sendRPC = send rpcMailbox,
          _sendRPCResult = send rpcResultsMailbox,
          _sendClientResponse = send responsesMailbox,
          _receiveRPC = receive rpcMailbox node <&> Right,
          _receiveRPCResult = receive rpcResultsMailbox node <&> Right,
          _receiveClientRequest = receive requestsMailbox node <&> Right,
          _tracer = traceM
        }

    clientSpec requestsMailbox responsesMailbox =
      MkRaftClientSpec
        { sendRequest = send requestsMailbox,
          receiveResponse =
            receive responsesMailbox clientNode <&> Right
        }

    send mailbox node message =
      atomically $ modifyTVar' mailbox (IntMap.insertWith (<>) node (Seq.singleton message))

    receive mailbox node = atomically $ do
      mail <- readTVar mailbox
      case IntMap.lookup node mail of
        Nothing -> retry
        Just Seq.Empty -> retry
        Just (nextMessage :<| rest) ->
          writeTVar mailbox (IntMap.insert node rest mail)
            >> pure nextMessage

-- Simple key-value store

type Node = Int

type State = Map Char Int

data Command
  = Insert Char Int
  | Delete Char
  | Get Char
  deriving (Eq, Ord, Show)

instance Arbitrary Command where
  arbitrary =
    let possibleKeys = ['a', 'b', 'c']
     in oneof
          [ Insert <$> elements possibleKeys <*> chooseInt (0, 10),
            Delete <$> elements possibleKeys,
            Get <$> elements possibleKeys
          ]

data Result
  = Value Int
  | Ok
  | Err
  deriving (Eq, Show)

step :: State -> Command -> (State, Result)
step state (Insert k v) = (Map.insert k v state, Ok)
step state (Delete k) = (Map.delete k state, Ok)
step state (Get k) = case Map.lookup k state of
  Nothing -> (state, Err)
  Just v -> (state, Value v)

expectedResults :: [Command] -> [(Command, State, Result)]
expectedResults [] = []
expectedResults allCommands@(cmd : cmds) =
  zipWith (\c (s, r) -> (c, s, r)) allCommands $
    reverse $
      snd $
        -- Using foldl' qualified to prevent
        -- warning of foldl' already being in prelude
        -- since GHC 9.10
        Foldable.foldl'
          ( \(state, results) c ->
              let (!newState, !result) = step state c in (newState, (state, result) : results)
          )
          (let (state, result) = step mempty cmd in (state, [(state, result)]))
          cmds
