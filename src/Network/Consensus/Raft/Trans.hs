{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE TemplateHaskell #-}

module Network.Consensus.Raft.Trans
  ( runRaftT,
    RaftT,
    Config (..),
    RaftEnv,
    spec,
    conf,
    eventQueue,

    -- * Events
    Event (..),

    -- * Capabilities
    dequeueEvent,
    enqueueEvent,

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

import Control.Concurrent.Class.MonadSTM (TQueue, atomically, newTQueue, readTQueue, writeTQueue)
import Control.Monad.Class.MonadSTM (MonadSTM)
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.RWS.CPS (RWST, ask, asks, evalRWST, get, gets, local, modify, put, state)
import Data.Int (Int32)
import Lens.Micro.Platform (makeLenses, view)
import Network.Consensus.Raft.Spec (RPC, RPCResult, RaftSpec, RaftState, initialRaftState)

type RaftT entry node result message m =
  RWST
    (RaftEnv entry node result message m)
    ()
    (RaftState node entry)
    m

runRaftT ::
  (MonadSTM m) =>
  Config node ->
  RaftSpec entry node result message m ->
  RaftT entry node result message m a ->
  m a
runRaftT c s f = do
  queue <- atomically newTQueue
  fst
    <$> evalRWST
      f
      ( MkRaftEnv
          { _conf = c,
            _spec = s,
            _eventQueue = queue
          }
      )
      initialRaftState

data Event node entry result
  = EventRPC (RPC node entry)
  | EventRPCResult (RPCResult node result)
  | EventElectionTimeout
  | EventHeartBeatTimeout

data RaftEnv entry node result message m
  = MkRaftEnv
  { _conf :: !(Config node),
    _spec :: !(RaftSpec entry node result message m),
    _eventQueue :: TQueue m (Event node entry result)
  }

newtype Microseconds = Microseconds Int32
  deriving stock (Eq, Ord, Show)
  deriving newtype (Real, Enum, Num, Integral)

data Config node
  = MkConfig
  { nodeId :: !node,
    electionTimeoutRange :: !(Microseconds, Microseconds),
    heartBeatTimeout :: !Microseconds
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
