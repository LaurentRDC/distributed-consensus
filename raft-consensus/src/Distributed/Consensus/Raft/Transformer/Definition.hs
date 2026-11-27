{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE TemplateHaskell #-}

module Distributed.Consensus.Raft.Transformer.Definition
  ( runRaftT,
    RaftT,
    Config (..),
    simpleConfig,
    ClusterState (..),
    RaftEnv,
    implementation,
    configuration,
    eventQueue,
    heartBeatTimer,
    electionTimer,
    exitLock,

    -- ** Read-only operations
    RaftSendT,
    runSend,

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
    pendingConfigurationIndex,
    yesVotes,
    currentClientRequests,
    pendingReadRequests,
    readOnlySequenceNum,
    snapshotInProgress,
    randomGen,

    -- ** Read-only client requests
    PendingRead (..),
    pendingReadTerm,
    pendingReadLogIndex,
    pendingReadClient,
    pendingReadLeaderAcknowledgedBy,
    pendingReadSequenceNum,
    pendingReadProjection,

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
import Control.Monad (void)
import Control.Monad.Class.MonadAsync (MonadAsync, async)
import Control.Monad.Class.MonadSTM (MonadSTM)
import Control.Monad.IO.Class (MonadIO)
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.RWS.CPS (RWST, ask, asks, evalRWST, get, gets, local, modify, put, state)
import Control.Monad.Trans.Reader (ReaderT, runReaderT)
import Data.Map.Strict (Map)
import Data.Set (Set)
import qualified Data.Set as Set
import Data.Word (Word64)
import Distributed.Consensus.Raft.Client (ClientRequestId)
import Distributed.Consensus.Raft.Domain (ClusterConfiguration (..), LogIndex, ReadOnlySequenceNum, Role (..), Term)
import Distributed.Consensus.Raft.Implementation (Event (..), Implementation, LogEntry)
import Distributed.Consensus.Raft.Log (Log, newLog)
import Distributed.Consensus.Raft.Timer (Microseconds, Timer, newTimer)
import Lens.Micro.Platform (makeLenses)
import System.Random (StdGen, mkStdGen64)
import System.Random.Stateful (randomIO)

type RaftT entry node result state m =
  -- Note: using RWST prevents to use the `MonadAsync`, `MonadSTM`, ...
  -- classes.
  -- The alternative would be to keep the 'RaftState' in a mutable variable
  -- (e.g. 'MVar'), but it's not clear how much of a performance or usability
  -- downgrade this would be.
  RWST
    (RaftEnv entry node result state m)
    ()
    (RaftState entry node result state)
    m

-- | This type exists so that we can spin out asynchronous threads
-- for sending messages
type RaftSendT entry node result state m =
  ReaderT
    (RaftEnv entry node result state m)
    m

-- | Run an action of type 'RaftSendT' asynchronously
runSend ::
  (MonadAsync m) =>
  RaftSendT entry node result state m () ->
  RaftT entry node result state m ()
runSend act = ask >>= lift . void . async . runReaderT act

data ClusterState node
  = -- | Lone node, not in a cluster. This 'ClusterState' is useful if you want
    -- to start up a Raft server and have it join an existing cluster.
    LoneNode
  | -- | Node in cluster. The cluster configuration can be empty,
    -- in which case this cluster has a single node
    InCluster (Set node)

data RaftState entry node result state = MkRaftState
  { _role :: !Role,
    _term :: !Term,
    _clusterConfiguration :: !(ClusterConfiguration node),
    _internalState :: !state,
    _votedFor :: !(Maybe node),
    _currentLeader :: !(Maybe node),
    _commandLog :: !(Log (LogEntry entry node) node),
    _commitIndex :: !LogIndex,
    _lastApplied :: !LogIndex,
    _nextIndex :: !(Map node LogIndex),
    _matchIndex :: !(Map node LogIndex),
    -- | Until this log index has been applied, a cluster configuration
    -- change should not be started, possibly because another cluster
    -- configuration change is ongoing.
    _pendingConfigurationIndex :: !LogIndex,
    -- | Set of votes received in the current term
    _yesVotes :: !(Set node),
    _currentClientRequests :: !(Map ClientRequestId node),
    _pendingReadRequests :: !(Map ClientRequestId (PendingRead node result state)),
    -- | Counter incremented on each incoming read-only query. This counter allows to know
    -- when a heartbeat counts to acknowledge a read-only request (or was already
    -- in-flight, for example)
    _readOnlySequenceNum :: !ReadOnlySequenceNum,
    -- | 'True' if a snapshot is currently being written to storage, which is done asynchronously.
    _snapshotInProgress :: !Bool,
    _randomGen :: !StdGen
  }

-- | A pending read-only request isn't part of the
-- log, so we need a little bit of extra metadata
data PendingRead node result state = MkPendingRead
  { _pendingReadTerm :: !Term,
    _pendingReadLogIndex :: !LogIndex,
    _pendingReadClient :: !node,
    _pendingReadSequenceNum :: !ReadOnlySequenceNum,
    -- After the request is accepted by the leader,
    -- we store nodes that have acknowledged us as the leader
    -- here. Once `hasQuorum` is reached, we can reply
    _pendingReadLeaderAcknowledgedBy :: !(Set node),
    _pendingReadProjection :: state -> result
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
  RaftState entry node result state
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
      _pendingConfigurationIndex = 0,
      _yesVotes = mempty,
      _currentClientRequests = mempty,
      _pendingReadRequests = mempty,
      _readOnlySequenceNum = 0,
      -- We use mkStdGen64 for reproducibility across 32-bit and 64-bit architectures
      _snapshotInProgress = False,
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
  Implementation entry node result state m ->
  RaftT entry node result state m a ->
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

data RaftEnv entry node result state m
  = MkRaftEnv
  { _configuration :: !(Config node),
    _implementation :: !(Implementation entry node result state m),
    _eventQueue :: TQueue m (Event entry node result state),
    -- Handle to a thread which will send a heartbeat timeout
    -- event after the appropriate amount of time.
    _heartBeatTimer :: Timer m,
    _electionTimer :: Timer m,
    _exitLock :: MVar m ()
  }

-- | Simplest configuration with good defaults:
--
-- * Election range between 1s and 2s
-- * Heartbeat timeout of 100ms
-- * Random seed
-- * Keep 100 000 log entries before compaction
simpleConfig :: (MonadIO m) => node -> m (Config node)
simpleConfig node = do
  seed <- randomIO
  pure $
    MkConfig
      { nodeId = node,
        electionTimeoutRange = (1_000_000, 2_000_000),
        heartBeatTimeout = 100_000,
        randomSeed = seed,
        maxLogLength = Just 100_000
      }

-- | Runtime configuration for a Raft server.
--
-- You should use 'simpleConfig' if you don't have strong opinions
-- on the configuration values.
data Config node
  = MkConfig
  { -- | Identification for the node
    nodeId :: !node,
    -- | Inclusive range of election timeouts. The actual election timeout will be
    -- chosen at random, with a uniform probability distribution in the range.
    electionTimeoutRange :: !(Microseconds, Microseconds),
    -- | Heartbeat timeout.
    heartBeatTimeout :: !Microseconds,
    -- | Random seed for the generation of election timeouts.
    --
    -- Make sure to not re-use a seed across servers. Re-using the same seed
    -- on multiple Raft servers will lead to permanently-split votes, rendering
    -- the cluster unable to process any requests.
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
makeLenses ''PendingRead
