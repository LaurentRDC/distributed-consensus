{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE TemplateHaskell #-}

module Network.Consensus.Raft.Transformer.Spec
  ( -- * Protocol specification
    Specification (..),

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
    AdminRequest,
    Event (..),
    RPC (..),
    RPCResult (..),
    EventContext (..),
    RaftTrace (..),
  )
where

import Data.Binary (Binary)
import Data.List.NonEmpty (NonEmpty)
import Data.Map.Strict (Map)
import Data.Sequence (Seq)
import Data.Set (Set)
import Data.Text (Text)
import Data.Word (Word64)
import GHC.Generics (Generic)
import Lens.Micro.Platform (makeLenses)
import Network.Consensus.Raft.Admin (AdminRequest, AdminResponse)
import Network.Consensus.Raft.Client (ClientRequest, ClientResponse)
import Network.Consensus.Raft.Domain (ClusterConfiguration (..), InternalRequestId, LogIndex, RequestId, Role (..), Snapshot, SnapshotMetadata, Term)
import Network.Consensus.Raft.Log (Log, newLog)
import System.Random (StdGen, mkStdGen64)

-- | A 'Command' comes from clients
data Command entry
  = Command
      -- | A 'RequestId' allows a leader to wait for a message
      --      before answering the client. This allows a leader to
      --      allow connections from multiple clients.
      !RequestId
      !entry
  deriving (Eq, Show, Ord, Generic)

instance (Binary entry) => Binary (Command entry)

-- | A 'LogEntry' is anything which gets persisted
-- in the replicated log. This includes client commands,
-- but also cluster membership changes.
data LogEntry node entry
  = LogEntryCommand (Command entry)
  | LogEntryMembershipChange (ClusterConfiguration node)
  deriving (Eq, Show, Ord, Generic)

instance (Binary node, Binary entry) => Binary (LogEntry node entry)

data CommandResponse node result
  = MkCommandResponse
      !result
      !RequestId
  deriving (Eq, Show, Ord, Generic)

instance (Binary node, Binary result) => Binary (CommandResponse node result)

data AppendEntries node entry = AppendEntries
  { aeLeaderTerm :: !Term,
    aeLeaderNode :: !node,
    aePreviousLogIndex :: !LogIndex,
    aePreviousLogTerm :: !Term,
    aeEntries :: !(Seq (Term, LogEntry node entry)),
    aeCommitIndex :: !LogIndex
  }
  deriving (Eq, Show, Ord, Generic)

instance (Binary node, Binary entry) => Binary (AppendEntries node entry)

data AppendEntriesResult node result = AppendEntriesResult
  { aerCurrentTerm :: !Term,
    aerNode :: !node,
    aerMatch :: !Bool,
    aerNewEntryLogIndex :: !LogIndex
  }
  deriving (Eq, Ord, Show, Generic)

instance (Binary node, Binary result) => Binary (AppendEntriesResult node result)

data InstallSnapshot node state = InstallSnapshot
  { isLeaderTerm :: !Term,
    isLeaderNode :: !node,
    isSnapshot :: !(Snapshot node state)
  }
  deriving (Eq, Ord, Show, Generic)

instance (Binary node, Binary state) => Binary (InstallSnapshot node state)

data InstallSnapshotResult node = InstallSnapshotResult
  { isrTerm :: !Term,
    isrNode :: !node,
    isrSnapshotMetadata :: !SnapshotMetadata
  }
  deriving (Eq, Ord, Show, Generic)

instance (Binary node) => Binary (InstallSnapshotResult node)

data ClusterMembershipRequest node
  = ClusterMembershipJoinRequest node
  | ClusterMembershipLeaveRequest node
  deriving (Eq, Ord, Show, Generic)

instance (Binary node) => Binary (ClusterMembershipRequest node)

data ClusterMembershipError node
  = -- | carries the node that replied, not the leader
    NoKnownLeader node
  | OngoingClusterMembershipChange node
  deriving (Eq, Show, Ord, Generic)

instance (Binary node) => Binary (ClusterMembershipError node)

data ClusterMembershipResult node
  = ClusterMembershipJoinResult (Either (ClusterMembershipError node) node)
  | ClusterMembershipLeaveResult (Either (ClusterMembershipError node) node)
  deriving (Eq, Ord, Show, Generic)

instance (Binary node) => Binary (ClusterMembershipResult node)

data RPC node entry state
  = AE (AppendEntries node entry)
  | IS (InstallSnapshot node state)
  | CM (ClusterMembershipRequest node)
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

instance (Binary node, Binary entry, Binary state) => Binary (RPC node entry state)

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

instance (Binary node, Binary result) => Binary (RPCResult node result)

data Event node entry result state
  = EventElectionTimeout
  | EventHeartBeatTimeout
  | EventIncomingClientRequest (NonEmpty (ClientRequest node entry))
  | EventRPC (RPC node entry state)
  | EventRPCResult (RPCResult node result)
  | EventAdminRequest (AdminRequest node)
  | EventSnapshotPersisted (Snapshot node state)
  deriving (Eq, Show)

data EventContext node
  = EventContext !Term !node
  deriving (Eq)

instance (Show node) => Show (EventContext node) where
  show (EventContext term node) = "[Term=" <> show term <> " | node=" <> show node <> "]"

data RaftTrace entry result node state
  = StateRestored (EventContext node) (Maybe node) (Log node (LogEntry node entry))
  | LeaderElected (EventContext node)
  | VotedFor (EventContext node) Term node
  | VoteRequestedBy (EventContext node) Term node
  | VoteGrantedFrom (EventContext node) node
  | VoteDeniedFrom (EventContext node) node
  | BecameCandidate (EventContext node)
  | BecameFollower (EventContext node)
  | BecameNonMember (EventContext node)
  | EventReceived (EventContext node) (Event node entry result state)
  | SplitElection (EventContext node)
  | ElectionTriggered (EventContext node)
  | DeserializationError node Text
  | -- | Command received by the leader node. If the command needs to be redirected
    --    to another node, this event is not emitted
    -- All Commands should have the same request ID
    CommandReceived (EventContext node) RequestId entry
  | CommandResultResponded (EventContext node) (CommandResponse node result)
  | -- TODO: better traching of new joining/leaving cluster
    MembershipChangeInitiated (EventContext node)
  | MembershipChangeCompleted (EventContext node)
  | AdminRequestReceived (EventContext node) (AdminRequest node)
  | JoinedCluster (EventContext node)
  | LeftCluster (EventContext node)
  | CommitIndexIncreasedTo (EventContext node) LogIndex
  | LogEntryAppended (EventContext node) LogIndex (LogEntry node entry)
  | LastLogIndexChangedTo (EventContext node) LogIndex
  | LogEntryApplied (EventContext node) entry
  | MembershipChangeApplied (EventContext node) (ClusterConfiguration node)
  | -- | A leader was asked for a membership change that its committed
    -- configuration already satisfies, so it did nothing.
    MembershipChangeAlreadySettled (EventContext node) node
  | LastAppliedIndexIncreasedTo (EventContext node) LogIndex
  | SnapshotApplied (EventContext node) SnapshotMetadata
  | GracefulShutdown (EventContext node)
  | -- | The following event is emitted by the fault injector
    -- in deterministic simulation tests. Users should not rely
    -- on tracing this event in production.
    Crashed node
  deriving (Eq, Show)

data Specification entry node state result m = Specification
  { -- TODO: allow to read and write multiple log entries at once, for performance optimizations
    readLogEntry :: node -> LogIndex -> m (Maybe (Term, LogEntry node entry)),
    writeLogEntry :: node -> LogIndex -> Term -> LogEntry node entry -> m (),
    readTerm :: node -> m Term,
    writeTerm :: node -> Term -> m (),
    readVotedFor :: node -> Term -> m (Maybe node),
    writeVotedFor :: node -> Term -> Maybe node -> m (),
    readSnapshot :: node -> m (Maybe (Snapshot node state)),
    writeSnapshot :: node -> Snapshot node state -> m (),
    applyLogEntry :: state -> entry -> (state, result),
    sendRPC :: node -> RPC node entry state -> m (),
    sendRPCResult :: node -> RPCResult node result -> m (),
    sendClientResponse :: node -> ClientResponse node result -> m (),
    sendAdminResponse :: node -> AdminResponse node -> m (),
    -- We use 'Text' to represent deserialization errors because this is
    -- easiest to represent to the user. I don't think it's worth adding
    -- yet another type variable to the spec.
    receiveRPC :: m (Either Text (RPC node entry state)),
    receiveRPCResult :: m (Either Text (RPCResult node result)),
    -- | Receive client requests.
    -- This call should block until at least one request is available. If
    -- your software stack allows for it, queueing requests allows
    -- for pipelined processing which is much more efficient.
    receiveClientRequests :: m (NonEmpty (ClientRequest node entry)),
    receiveAdminRequest :: m (Either Text (AdminRequest node)),
    tracer :: RaftTrace entry result node state -> m ()
  }

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

makeLenses ''RaftState

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
