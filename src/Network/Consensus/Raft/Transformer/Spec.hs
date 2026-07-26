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
    applyLogEntry,
    serializeRPC,
    serializeRPCResult,
    deserializeRPC,
    deserializeRPCResult,
    send,
    receive,
    tracer,

    -- * Raft state
    RaftState,
    initialTerm,
    initialRaftState,
    role,
    term,
    internalState,
    votedFor,
    currentLeader,
    logEntries,
    commitIndex,
    lastApplied,
    nextIndex,
    matchIndex,
    yesVotes,
    randomGen,

    -- * Types
    Command (..),
    CommandResponse (..),
    AppendEntries (..),
    AppendEntriesResult (..),
    RPC (..),
    RPCResult (..),
    Role (..),
    LogIndex,
    Term,
    RequestId,
    RaftTrace (..),
  )
where

import Data.Int (Int64)
import Data.Map.Strict (Map)
import Data.Sequence (Seq)
import Data.Set (Set)
import Data.Text (Text)
import Data.Vector (Vector)
import Data.Word (Word64)
import GHC.Generics (Generic)
import Lens.Micro.Platform (makeLenses)
import System.Random (StdGen, mkStdGen64)

newtype LogIndex = LogIndex Int64
  deriving stock (Generic, Eq, Ord, Show)
  deriving newtype (Real, Enum, Num, Integral)

newtype Term = Term Int64
  deriving stock (Generic, Eq, Ord, Show)
  deriving newtype (Real, Enum, Num, Integral)

newtype RequestId = RequestId Int64
  deriving stock (Generic, Eq, Ord, Show)
  deriving newtype (Real, Enum, Num, Integral)

data Command node entry
  = MkCommand
      -- | Client node ID, to reply
      !node
      !entry
      -- | A 'RequestId' allows a client to correlate a command with its response,
      -- in the event that a client issues multiple commands
      !RequestId
  deriving (Eq, Show, Ord, Generic)

data CommandResponse node result
  = MkCommandResponse
      -- | Leader node ID, for further requests
      !node
      !result
      !RequestId
  deriving (Eq, Show, Ord, Generic)

data AppendEntries node entry = AppendEntries
  { aeLeaderTerm :: !Term,
    aeLeaderNode :: !node,
    aePreviousLogIndex :: !LogIndex,
    aePreviousLogTerm :: !Term,
    aeEntries :: !(Vector (Term, Command node entry)),
    aeCommitIndex :: !LogIndex
  }
  deriving (Eq, Show, Ord, Generic)

data AppendEntriesResult node result = AppendEntriesResult
  { aerCurrentTerm :: !Term,
    aerNode :: !node,
    aerMatch :: !Bool,
    aerNewEntryLogIndex :: !LogIndex
  }
  deriving (Eq, Ord, Show, Generic) -- For easy derivation of de/serialization

data RPC node entry
  = ClientRequest (Command node entry)
  | AE (AppendEntries node entry)
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
  = ClientRequestResult (CommandResponse node result)
  | AER (AppendEntriesResult node result)
  | RequestVoteResult
      -- | Voter node
      node
      -- | Current term, for leader to update itself
      Term
      -- | Whether vote was granted
      Bool
  deriving (Eq, Ord, Show, Generic) -- For easy derivation of de/serialization

data RaftTrace entry result node
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
  | RPCReceived Term node (RPC node entry)
  | RPCResultReceived Term node (RPCResult node result)
  | SplitElection Term node
  | ElectionTriggered Term node
  | DeserializationError node Text
  | -- | Command received by the leader node. If the command needs to be redirected
    -- to another node, this event is not emitted
    CommandReceived Term node (Command node entry)
  | CommandResultResponded Term node (CommandResponse node result)
  | CommitIndexIncreasedTo Term node LogIndex
  | LogEntryApplied Term node entry
  deriving (Eq, Ord, Show)

data RaftSpec entry node state result message m = MkRaftSpec
  { _readLogEntry :: LogIndex -> m (Maybe entry),
    _writeLogEntry :: LogIndex -> Term -> entry -> m (),
    _readTerm :: m Term,
    _writeTerm :: Term -> m (),
    _readVotedFor :: m (Maybe node),
    _voteFor :: Maybe node -> m (),
    _applyLogEntry :: state -> entry -> (state, result),
    _serializeRPC :: RPC node entry -> message,
    _serializeRPCResult :: RPCResult node result -> message,
    -- We use 'Text' to represent deserialization errors because this is
    -- easiest to represent to the user. I don't think it's worth adding
    -- yet another type variable to the spec.
    _deserializeRPC :: message -> Either Text (RPC node entry),
    _deserializeRPCResult :: message -> Either Text (RPCResult node result),
    _send :: node -> message -> m (),
    _receive :: m message,
    _tracer :: RaftTrace entry result node -> m ()
  }

makeLenses ''RaftSpec

data Role
  = Leader
  | Follower
  | Candidate
  deriving (Eq, Show, Ord, Enum, Bounded)

data RaftState node entry state = MkRaftState
  { _role :: !Role,
    _term :: !Term,
    _internalState :: !state,
    _votedFor :: !(Maybe node),
    _currentLeader :: !(Maybe node),
    _logEntries :: !(Seq (Term, Command node entry)),
    _commitIndex :: !LogIndex,
    _lastApplied :: !LogIndex,
    _nextIndex :: Map node LogIndex,
    _matchIndex :: Map node LogIndex,
    -- | Set of votes received in the current term
    _yesVotes :: !(Set node),
    _randomGen :: !StdGen
  }

makeLenses ''RaftState

initialTerm :: Term
initialTerm = 1

initialRaftState :: (Ord node) => Word64 -> state -> RaftState node entry state
initialRaftState seed initialState =
  MkRaftState
    { _role = Follower,
      _term = initialTerm,
      _internalState = initialState,
      _votedFor = Nothing,
      _currentLeader = Nothing,
      _logEntries = mempty,
      _commitIndex = 0,
      _lastApplied = 0,
      _nextIndex = mempty,
      _matchIndex = mempty,
      _yesVotes = mempty,
      -- We use mkStdGen64 for reproducibility across 32-bit and 64-bit architectures
      _randomGen = mkStdGen64 seed
    }
