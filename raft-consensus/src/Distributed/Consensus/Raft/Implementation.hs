{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE ExistentialQuantification #-}

module Distributed.Consensus.Raft.Implementation
  ( -- * Protocol implementation
    Implementation (..),
    Networking (..),
    Persistence (..),

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
import Data.Sequence (Seq)
import Distributed.Consensus.Raft.Admin (AdminRequest, AdminResponse)
import Distributed.Consensus.Raft.Client (ClientRequest, ClientResponse)
import Distributed.Consensus.Raft.Domain (ClusterConfiguration (..), LogIndex, RequestId, Snapshot, SnapshotMetadata, Term)
import Distributed.Consensus.Raft.Log (Log)
import GHC.Generics (Generic)

data Implementation entry node state result m = Implementation
  { persistence :: Persistence entry node state m,
    applyLogEntry :: state -> entry -> (state, result),
    networking :: Networking entry node state result m,
    tracer :: RaftTrace entry result node state -> m ()
  }

-- | Networking implementation.
--
-- This is broken out into its own type so that it can be provided cleanly by
-- third-party packages
data Networking entry node state result m = Networking
  { sendRPC :: node -> RPC node entry state -> m (),
    sendRPCResult :: node -> RPCResult node result -> m (),
    sendClientResponse :: node -> ClientResponse node result -> m (),
    sendAdminResponse :: node -> AdminResponse node -> m (),
    -- We use 'Text' to represent deserialization errors because this is
    -- easiest to represent to the user. I don't think it's worth adding
    -- yet another type variable to the impl.
    receiveRPC :: m (RPC node entry state),
    receiveRPCResult :: m (RPCResult node result),
    -- | Receive client requests.
    -- This call should block until at least one request is available. If
    -- your software stack allows for it, queueing requests allows
    -- for pipelined processing which is much more efficient.
    receiveClientRequests :: m (NonEmpty (ClientRequest node entry)),
    receiveAdminRequest :: m (AdminRequest node)
  }

-- | Persistence implementation.
--
-- This is broken out into its own type so that it can be provided cleanly by
-- third-party packages
data Persistence entry node state m = Persistence
  { -- | Read log entries, starting from and including, a given 'LogIndex'
    readLogEntriesFrom :: node -> LogIndex -> m [(Term, LogEntry node entry)],
    writeLogEntry :: node -> [(LogIndex, Term, LogEntry node entry)] -> m (),
    readTerm :: node -> m Term,
    writeTerm :: node -> Term -> m (),
    readVotedFor :: node -> Term -> m (Maybe node),
    writeVotedFor :: node -> Term -> Maybe node -> m (),
    readSnapshot :: node -> m (Maybe (Snapshot node state)),
    writeSnapshot :: node -> Snapshot node state -> m ()
  }

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
