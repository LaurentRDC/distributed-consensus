{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeSynonymInstances #-}

module Network.Consensus.Raft.Trans
  ( runRaftT,
    RaftT,
    Config (..),
    RaftEnv,
    specification,
    configuration,
    eventQueue,
    heartBeatTimer,
    electionTimer,

    -- * Events
    Event (..),

    -- * Capabilities
    dequeueEvent,
    enqueueEvent,
    sendRPC,
    sendRPCResult,
    quorum,
    nextElectionTimeout,
    trace,

    -- * Helpers
    ask,
    asks,
    local,
    state,
    get,
    put,
    modify,
    gets,
  )
where

import Control.Concurrent.Class.MonadMVar (MonadMVar)
import Control.Concurrent.Class.MonadSTM (TQueue, atomically, newTQueue, readTQueue, writeTQueue)
import Control.Monad.Class.MonadSTM (MonadSTM)
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.RWS.CPS (RWST, ask, asks, evalRWST, get, gets, local, modify, put, state)
import Data.Functor ((<&>))
import Data.Set (Set)
import qualified Data.Set as Set
import Data.Word (Word64)
import Lens.Micro.Platform (makeLenses, use, view, (.=), (^.))
import Network.Consensus.Raft.Spec (RPC, RPCResult, RaftSpec, RaftState, RaftTrace, initialRaftState, randomGen, send, serializeRPC, serializeRPCResult, tracer)
import Network.Consensus.Raft.Timer (Microseconds, Timer, newTimer)
import System.Random (uniformR)

type RaftT entry node result message m =
  RWST
    (RaftEnv entry node result message m)
    ()
    (RaftState node entry)
    m

runRaftT ::
  ( Ord node,
    MonadSTM m,
    MonadMVar m
  ) =>
  Config node ->
  RaftSpec entry node result message m ->
  RaftT entry node result message m a ->
  m a
runRaftT c s f = do
  queue <- atomically newTQueue
  hbTimer <- newTimer (atomically $ writeTQueue queue EventHeartBeatTimeout)
  elTimer <- newTimer (atomically $ writeTQueue queue EventElectionTimeout)
  fst
    <$> evalRWST
      f
      ( MkRaftEnv
          { _configuration = c,
            _specification = s,
            _eventQueue = queue,
            _heartBeatTimer = hbTimer,
            _electionTimer = elTimer
          }
      )
      (initialRaftState (randomSeed c))

data Event node entry result
  = EventRPC (RPC node entry)
  | EventRPCResult (RPCResult node result)
  | EventElectionTimeout
  | EventHeartBeatTimeout

data RaftEnv entry node result message m
  = MkRaftEnv
  { _configuration :: !(Config node),
    _specification :: !(RaftSpec entry node result message m),
    _eventQueue :: TQueue m (Event node entry result),
    -- Handle to a thread which will send a heartbeat timeout
    -- event after the appropriate amount of time.
    _heartBeatTimer :: Timer m,
    _electionTimer :: Timer m
  }

data Config node
  = MkConfig
  { nodeId :: !node,
    otherNodes :: !(Set node),
    electionTimeoutRange :: !(Microseconds, Microseconds),
    heartBeatTimeout :: !Microseconds,
    randomSeed :: !Word64
  }

makeLenses ''RaftEnv

dequeueEvent :: (MonadSTM m) => RaftT entry node result message m (Event node entry result)
dequeueEvent = do
  queue <- view eventQueue
  lift $ atomically $ readTQueue queue

enqueueEvent :: (MonadSTM m) => Event node entry result -> RaftT entry node result message m ()
enqueueEvent event = do
  queue <- view eventQueue
  lift $ atomically $ writeTQueue queue event

sendRPC :: (Monad m) => node -> RPC node entry -> RaftT entry node result message m ()
sendRPC node rpc = do
  spec <- view specification
  sendMessage node ((spec ^. serializeRPC) rpc)

sendRPCResult :: (Monad m) => node -> RPCResult node result -> RaftT entry node result message m ()
sendRPCResult node rpc = do
  spec <- view specification
  sendMessage node ((spec ^. serializeRPCResult) rpc)

sendMessage :: (Monad m) => node -> message -> RaftT entry node result message m ()
sendMessage n m = do
  spec <- view specification
  lift $ (spec ^. send) n m

-- | Number of votes required for a decision to be majority.
quorum :: (Monad m) => RaftT entry node result message m Int
quorum = do
  conf <- view configuration
  let numNodesInCluster = 1 + Set.size (otherNodes conf)
  pure $
    if even numNodesInCluster
      then numNodesInCluster `div` 2 + 1
      else (numNodesInCluster - 1) `div` 2 + 1

nextElectionTimeout :: (Monad m) => RaftT entry node result message m Microseconds
nextElectionTimeout = do
  gen <- use randomGen
  (lowerBound, upperBound) <- view configuration <&> electionTimeoutRange
  let (timeout, nextGen) = uniformR (lowerBound, upperBound) gen
  randomGen .= nextGen
  pure timeout

trace :: (Monad m) => RaftTrace node -> RaftT entry node result message m ()
trace ev = do
  spec <- view specification
  lift $ (spec ^. tracer) ev
