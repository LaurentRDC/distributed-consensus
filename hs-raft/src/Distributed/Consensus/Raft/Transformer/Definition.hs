{-# LANGUAGE TemplateHaskell #-}

module Distributed.Consensus.Raft.Transformer.Definition
  ( runRaftT,
    RaftT,
    Config (..),
    ClusterState (..),
    RaftEnv,
    implementation,
    configuration,
    eventQueue,
    heartBeatTimer,
    electionTimer,
    exitLock,

    -- * Internal Raft state
    RaftState,
    initialTerm,
    initialRaftState,
    role,
    term,
    clusterConfiguration,
    internalState,
    votedFor,
    currentLeader,
    commandLog,
    commitIndex,
    lastApplied,
    nextIndex,
    matchIndex,
    yesVotes,
    nextRequestId,
    currentClientRequests,
    randomGen,

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
import Data.Map.Strict (Map)
import Data.Set (Set)
import qualified Data.Set as Set
import Data.Word (Word64)
import Distributed.Consensus.Raft.Domain (ClusterConfiguration (..), InternalRequestId, LogIndex, RequestId, Role (..), Term)
import Distributed.Consensus.Raft.Implementation (Event (..), Implementation, LogEntry)
import Distributed.Consensus.Raft.Log (Log, newLog)
import Distributed.Consensus.Raft.Timer (Microseconds, Timer, newTimer)
import Lens.Micro.Platform (makeLenses)
import System.Random (StdGen, mkStdGen64)

type RaftT entry node state result m =
  -- Note: using RWST prevents to use the `MonadAsync`, `MonadSTM`, ...
  -- classes.
  -- The alternative would be to keep the 'RaftState' in a mutable variable
  -- (e.g. 'MVar'), but it's not clear how much of a performance or usability
  -- downgrade this would be.
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

data RaftState node entry state = MkRaftState
  { _role :: !Role,
    _term :: !Term,
    _clusterConfiguration :: !(ClusterConfiguration node),
    _internalState :: !state,
    _votedFor :: !(Maybe node),
    _currentLeader :: !(Maybe node),
    _commandLog :: !(Log node (LogEntry node entry)),
    _commitIndex :: !LogIndex,
    _lastApplied :: !LogIndex,
    _nextIndex :: !(Map node LogIndex),
    _matchIndex :: !(Map node LogIndex),
    -- | Set of votes received in the current term
    _yesVotes :: !(Set node),
    _nextRequestId :: !InternalRequestId,
    _currentClientRequests :: !(Map RequestId node),
    _randomGen :: !StdGen
  }

initialTerm :: Term
initialTerm = 1

initialRaftState ::
  (Ord node) =>
  Role ->
  -- | Random number generator seed
  Word64 ->
  -- | Initial cluster configuration
  Set node ->
  -- | Initial internal state
  state ->
  RaftState node entry state
initialRaftState initRole seed clusterConf initialState =
  MkRaftState
    { _role = initRole,
      _term = initialTerm,
      _clusterConfiguration = Simple clusterConf,
      _internalState = initialState,
      _votedFor = Nothing,
      _currentLeader = Nothing,
      _commandLog = newLog,
      _commitIndex = 0,
      _lastApplied = 0,
      _nextIndex = mempty,
      _matchIndex = mempty,
      _yesVotes = mempty,
      _nextRequestId = 0,
      _currentClientRequests = mempty,
      -- We use mkStdGen64 for reproducibility across 32-bit and 64-bit architectures
      _randomGen = mkStdGen64 seed
    }

runRaftT ::
  ( Ord node,
    MonadSTM m,
    MonadMVar m
  ) =>
  Config node ->
  ClusterState node ->
  state ->
  Implementation entry node state result m ->
  RaftT entry node state result m a ->
  m a
runRaftT config startingState internalState impl f = do
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
            _implementation = impl,
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
    _implementation :: !(Implementation entry node state result m),
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
makeLenses ''RaftState
