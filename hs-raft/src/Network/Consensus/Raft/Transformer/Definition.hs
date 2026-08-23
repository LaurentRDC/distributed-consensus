{-# LANGUAGE TemplateHaskell #-}

module Network.Consensus.Raft.Transformer.Definition
  ( runRaftT,
    RaftT,
    Config (..),
    ClusterState (..),
    RaftEnv,
    specification,
    configuration,
    eventQueue,
    heartBeatTimer,
    electionTimer,
    exitLock,

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

import Control.Concurrent.Class.MonadMVar (MVar, MonadMVar, newEmptyMVar)
import Control.Concurrent.Class.MonadSTM (TQueue, atomically, newTQueue, writeTQueue)
import Control.Monad.Class.MonadSTM (MonadSTM)
import Control.Monad.Trans.RWS.CPS (RWST, ask, asks, evalRWST, get, gets, local, modify, put, state)
import Data.Set (Set)
import qualified Data.Set as Set
import Data.Word (Word64)
import Lens.Micro.Platform (makeLenses)
import Network.Consensus.Raft.Domain (Role (..))
import Network.Consensus.Raft.Timer (Microseconds, Timer, newTimer)
import Network.Consensus.Raft.Transformer.Spec (Event (..), RaftState, Specification, initialRaftState)

type RaftT entry node state result m =
  RWST
    (RaftEnv entry node state result m)
    ()
    (RaftState node entry state)
    m

data ClusterState node
  = -- | Lone node, not in a cluster
    LoneNode
  | -- | Node in cluster. The cluster configuration can be empty,
    -- in which case this cluster has a single node
    InCluster (Set node)

runRaftT ::
  ( Ord node,
    MonadSTM m,
    MonadMVar m
  ) =>
  Config node ->
  ClusterState node ->
  state ->
  Specification entry node state result m ->
  RaftT entry node state result m a ->
  m a
runRaftT config startingState internalState spec f = do
  queue <- atomically newTQueue
  hbTimer <- newTimer (atomically $ writeTQueue queue EventHeartBeatTimeout)
  elTimer <- newTimer (atomically $ writeTQueue queue EventElectionTimeout)
  exitLock <- newEmptyMVar
  let (members, initRole) = case startingState of
        LoneNode -> (mempty, NonMember)
        InCluster cluster -> (Set.insert (nodeId config) cluster, Follower)
  fst
    <$> evalRWST
      f
      ( MkRaftEnv
          { _configuration = config,
            _specification = spec,
            _eventQueue = queue,
            _heartBeatTimer = hbTimer,
            _electionTimer = elTimer,
            _exitLock = exitLock
          }
      )
      (initialRaftState initRole (randomSeed config) members internalState)

data RaftEnv entry node state result m
  = MkRaftEnv
  { _configuration :: !(Config node),
    _specification :: !(Specification entry node state result m),
    _eventQueue :: TQueue m (Event node entry result state),
    -- Handle to a thread which will send a heartbeat timeout
    -- event after the appropriate amount of time.
    _heartBeatTimer :: Timer m,
    _electionTimer :: Timer m,
    _exitLock :: MVar m ()
  }

data Config node
  = MkConfig
  { nodeId :: !node,
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
