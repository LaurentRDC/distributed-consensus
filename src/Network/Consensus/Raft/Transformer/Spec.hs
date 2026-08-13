{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE TemplateHaskell #-}

module Network.Consensus.Raft.Transformer.Spec
  ( -- * Protocol specification
    RaftSpec (..),
    readLogEntry,
    writeLogEntry,
    readTerm,
    writeTerm,
    readVotedFor,
    voteFor,
    writeSnapshot,
    readSnapshot,
    applyLogEntry,
    sendRPC,
    sendRPCResult,
    sendClientResponse,
    receiveRPC,
    receiveRPCResult,
    receiveClientRequest,
    tracer,

    -- * Raft state
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

    -- * Types
    LogEntry (..),
    Command (..),
    CommandResponse (..),
    AppendEntries (..),
    AppendEntriesResult (..),
    InstallSnapshot (..),
    InstallSnapshotResult (..),
    ClusterMembershipRequest (..),
    ClusterMembershipResult (..),
    ClusterMembershipError (..),
    Event (..),
    RPC (..),
    RPCResult (..),
    RaftTrace (..),
  )
where

import Data.Map.Strict (Map)
import Data.Set (Set)
import Data.Text (Text)
import Data.Vector (Vector)
import Data.Word (Word64)
import GHC.Generics (Generic)
import Lens.Micro.Platform (makeLenses)
import Network.Consensus.Raft.Client (Request, Response)
import Network.Consensus.Raft.Domain (ClusterConfiguration (..), RequestId, Role (..), Term)
import Network.Consensus.Raft.Log (Log, LogIndex, Snapshot, SnapshotMetadata, newLog)
import System.Random (StdGen, mkStdGen64)

-- | A 'Command' comes from clients
data Command entry
  = Command
      !entry
      -- | A 'RequestId' allows a leader to wait for a message
      --      before answering the client. This allows a leader to
      --      allow connections from multiple clients.
      !RequestId
  deriving (Eq, Show, Ord, Generic)

-- | A 'LogEntry' is anything which gets persisted
-- in the replicated log. This includes client commands,
-- but also cluster membership changes.
data LogEntry node entry
  = LogEntryCommand (Command entry)
  | LogEntryMembershipChange (ClusterConfiguration node)
  deriving (Eq, Show, Ord, Generic)

data CommandResponse node result
  = MkCommandResponse
      !result
      !RequestId
  deriving (Eq, Show, Ord, Generic)

data AppendEntries node entry = AppendEntries
  { aeLeaderTerm :: !Term,
    aeLeaderNode :: !node,
    aePreviousLogIndex :: !LogIndex,
    aePreviousLogTerm :: !Term,
    aeEntries :: !(Vector (Term, LogEntry node entry)),
    aeCommitIndex :: !LogIndex
  }
  deriving (Eq, Show, Ord, Generic)

data AppendEntriesResult node result = AppendEntriesResult
  { aerCurrentTerm :: !Term,
    aerNode :: !node,
    aerMatch :: !Bool,
    aerNewEntryLogIndex :: !LogIndex
  }
  deriving (Eq, Ord, Show, Generic)

data InstallSnapshot node state = InstallSnapshot
  { isLeaderTerm :: !Term,
    isLeaderNode :: !node,
    isSnapshot :: !(Snapshot node state)
  }
  deriving (Eq, Ord, Show, Generic)

data InstallSnapshotResult node = InstallSnapshotResult
  { isrTerm :: !Term,
    isrNode :: !node,
    isrSnapshotMetadata :: !SnapshotMetadata
  }
  deriving (Eq, Ord, Show, Generic)

newtype ClusterMembershipRequest node = ClusterMembershipRequest
  {cmrRequester :: node}
  deriving (Eq, Ord, Show, Generic)

data ClusterMembershipError node
  = -- | carries the node that replied, not the leader
    NoKnownLeader node
  | OngoingClusterMembershipChange node
  deriving (Eq, Show, Ord, Generic)

newtype ClusterMembershipResult node = ClusterMembershipResult
  { cmrLeader :: Either (ClusterMembershipError node) node
  }
  deriving (Eq, Ord, Show, Generic)

data RPC node entry state
  = AE (AppendEntries node entry)
  | IS (InstallSnapshot node state)
  | CM (ClusterMembershipRequest node)
  | HeartBeat
      -- | Leader's term
      Term
      -- | Identification of the leader
      node
      -- | Previous log index
      LogIndex
      -- | Commit index
      LogIndex
  | RequestVote
      -- | Candidate term
      Term
      -- | Identification of the candidate requesting vote
      node
      -- | Index of candidate's last log entry
      LogIndex
      -- | Term of candidate's last log entry
      Term
  deriving (Eq, Ord, Show, Generic) -- For easy derivation of de/serialization

data RPCResult node result
  = AER (AppendEntriesResult node result)
  | ISR (InstallSnapshotResult node)
  | CMR (ClusterMembershipResult node)
  | RequestVoteResult
      -- | Voter node
      node
      -- | Current term, for leader to update itself
      Term
      -- | Whether vote was granted
      Bool
  deriving (Eq, Ord, Show, Generic) -- For easy derivation of de/serialization

data Event node entry result state
  = EventElectionTimeout
  | EventHeartBeatTimeout
  | EventIncomingClientRequest (Request node entry)
  | EventRPC (RPC node entry state)
  | EventRPCResult (RPCResult node result)
  deriving (Eq, Show)

data RaftTrace entry result node state
  = LeaderElected Term node
  | VotedFor
      -- | Our term
      Term
      -- | Our node
      node
      -- | Their term
      Term
      -- | Their node
      node
  | VoteRequestedBy
      -- | Our term
      Term
      -- | Our node
      node
      -- | Their term
      Term
      -- | Their node
      node
  | VoteGrantedFrom
      -- | Our term
      Term
      -- | Our node
      node
      -- | Their node
      node
  | VoteDeniedFrom
      -- | Our term
      Term
      -- | Our node
      node
      -- | Their node
      node
  | BecameCandidate Term node
  | BecameFollower Term node
  | EventReceived Term node (Event node entry result state)
  | SplitElection Term node
  | ElectionTriggered Term node
  | DeserializationError node Text
  | -- | Command received by the leader node. If the command needs to be redirected
    --    to another node, this event is not emitted
    CommandReceived Term node (Command entry)
  | CommandResultResponded Term node (CommandResponse node result)
  | -- TODO: better traching of new joining/leaving cluster
    MembershipChangeInitiated Term node
  | MembershipChangeCompleted Term node
  | JoinedCluster Term node
  | CommitIndexIncreasedTo Term node LogIndex
  | LogEntryAppended Term node (LogEntry node entry)
  | LogEntryApplied Term node entry
  | MembershipChangeApplied Term node (ClusterConfiguration node)
  | LastAppliedIndexIncreasedTo Term node LogIndex
  | SnapshotApplied Term node SnapshotMetadata
  deriving (Eq, Show)

data RaftSpec entry node state result m = MkRaftSpec
  { _readLogEntry :: node -> LogIndex -> m (Maybe entry),
    _writeLogEntry :: node -> LogIndex -> Term -> entry -> m (),
    _readTerm :: node -> m Term,
    _writeTerm :: node -> Term -> m (),
    _readVotedFor :: node -> m (Maybe node),
    _voteFor :: node -> Maybe node -> m (),
    _readSnapshot :: node -> m (Maybe (Snapshot node state)),
    _writeSnapshot :: node -> Snapshot node state -> m (),
    _applyLogEntry :: state -> entry -> (state, result),
    _sendRPC :: node -> RPC node entry state -> m (),
    _sendRPCResult :: node -> RPCResult node result -> m (),
    _sendClientResponse :: node -> Response node result -> m (),
    -- We use 'Text' to represent deserialization errors because this is
    -- easiest to represent to the user. I don't think it's worth adding
    -- yet another type variable to the spec.
    _receiveRPC :: m (Either Text (RPC node entry state)),
    _receiveRPCResult :: m (Either Text (RPCResult node result)),
    _receiveClientRequest :: m (Either Text (Request node entry)),
    _tracer :: RaftTrace entry result node state -> m ()
  }

makeLenses ''RaftSpec

data RaftState node entry state = MkRaftState
  { _role :: !Role,
    _term :: !Term,
    _clusterConfiguration :: !(ClusterConfiguration node),
    _internalState :: !state,
    _votedFor :: !(Maybe node),
    _currentLeader :: !(Maybe node),
    _commandLog :: !(Log node state (Term, LogEntry node entry)),
    _commitIndex :: !LogIndex,
    _lastApplied :: !LogIndex,
    _nextIndex :: Map node LogIndex,
    _matchIndex :: Map node LogIndex,
    -- | Set of votes received in the current term
    _yesVotes :: !(Set node),
    _nextRequestId :: !RequestId,
    _currentClientRequests :: !(Map RequestId node),
    _randomGen :: !StdGen
  }

makeLenses ''RaftState

initialTerm :: Term
initialTerm = 1

initialRaftState ::
  (Ord node) =>
  -- | Random number generator seed
  Word64 ->
  -- | Initial cluster configuration
  Set node ->
  -- | Initial internal state
  state ->
  RaftState node entry state
initialRaftState seed clusterConf initialState =
  MkRaftState
    { _role = Follower,
      _term = initialTerm,
      _clusterConfiguration = Simple clusterConf,
      _internalState = initialState,
      _votedFor = Nothing,
      _currentLeader = Nothing,
      _commandLog = newLog initialState clusterConf,
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
