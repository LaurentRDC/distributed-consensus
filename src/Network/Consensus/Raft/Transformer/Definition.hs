{-# LANGUAGE TemplateHaskell #-}

module Network.Consensus.Raft.Transformer.Definition
  ( runRaftT,
    RaftT,
    Config (..),
    RaftEnv,
    specification,
    configuration,
    eventQueue,
    currentClientRequests,
    heartBeatTimer,
    electionTimer,

    -- * Generic helpers
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

import Control.Concurrent.Class.MonadMVar (MVar, MonadMVar, newMVar)
import Control.Concurrent.Class.MonadSTM (TQueue, atomically, newTQueue, writeTQueue)
import Control.Monad.Class.MonadSTM (MonadSTM)
import Control.Monad.Trans.RWS.CPS (RWST, ask, asks, evalRWST, get, gets, local, modify, put, state)
import Data.IntMap.Strict (IntMap)
import Data.Set (Set)
import Data.Word (Word64)
import Lens.Micro.Platform (makeLenses)
import Network.Consensus.Raft.Timer (Microseconds, Timer, newTimer)
import Network.Consensus.Raft.Transformer.Spec (Event (..), RaftSpec, RaftState, initialRaftState)

type RaftT entry node state result m =
  RWST
    (RaftEnv entry node state result m)
    ()
    (RaftState node entry state)
    m

runRaftT ::
  ( Ord node,
    MonadSTM m,
    MonadMVar m
  ) =>
  Config node ->
  state ->
  RaftSpec entry node state result m ->
  RaftT entry node state result m a ->
  m a
runRaftT c i s f = do
  queue <- atomically newTQueue
  requests <- newMVar mempty
  hbTimer <- newTimer (atomically $ writeTQueue queue EventHeartBeatTimeout)
  elTimer <- newTimer (atomically $ writeTQueue queue EventElectionTimeout)
  fst
    <$> evalRWST
      f
      ( MkRaftEnv
          { _configuration = c,
            _specification = s,
            _eventQueue = queue,
            _currentClientRequests = requests,
            _heartBeatTimer = hbTimer,
            _electionTimer = elTimer
          }
      )
      (initialRaftState (randomSeed c) i)

data RaftEnv entry node state result m
  = MkRaftEnv
  { _configuration :: !(Config node),
    _specification :: !(RaftSpec entry node state result m),
    _eventQueue :: TQueue m (Event node entry result state),
    _currentClientRequests :: MVar m (IntMap (MVar m result)),
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
    randomSeed :: !Word64,
    -- | If provided, once the log on any node
    -- reaches this length, a snapshot is produced
    -- for log compaction. If @Nothing@, snapshotting
    -- is disabled.
    maxLogLength :: Maybe Int
    -- TODO: configure the maximum amount of concurrency. For example, we currently
    --       spawn a new thread to send an RPC to each other node.
    --       We may want to batch this concurrency via something like
    --       `Control.Concurrent.Stream.mapConcurrentlyBounded`
  }

makeLenses ''RaftEnv
