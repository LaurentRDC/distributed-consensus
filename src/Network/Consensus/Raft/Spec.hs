{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE TemplateHaskell #-}

module Network.Consensus.Raft.Spec
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

    -- * Raft state
    RaftState,
    initialRaftState,
    role,
    term,
    votedFor,
    currentLeader,
    logEntries,
    commitIndex,
    lastApplied,

    -- * Types

    RPC (..),
    RPCResult (..),
    Role (..),
    LogIndex,
    Term,
  )
where

import Data.Int (Int64)
import Data.Sequence (Seq)
import Data.Vector (Vector)
import Lens.Micro.Platform (makeLenses)

newtype LogIndex = LogIndex Int64
  deriving stock (Eq, Ord, Show)
  deriving newtype (Real, Enum, Num, Integral)

newtype Term = Term Int64
  deriving stock (Eq, Ord, Show)
  deriving newtype (Real, Enum, Num, Integral)

newtype RequestId = RequestId Int64
  deriving stock (Eq, Ord, Show)
  deriving newtype (Real, Enum, Num, Integral)

data RPC node entry
  = AppendEntries
      -- | Leader's term
      Term
      -- | Identification of the leader
      node
      -- | Previous log index
      LogIndex
      -- | Entries (empty for heartbeat)
      (Vector entry)
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
  | Command
      -- | Client
      node
      -- | Command
      entry
      -- | Request identifier
      RequestId

data RPCResult node result
  = AppendEntriesResult
      -- | Current term, for leader to update itself
      Term
      -- | Whether the following contained entry matching the previous log index and previous log term
      Bool
  | RequestVoteResult
      -- | Current term, for leader to update itself
      Term
      -- | Whether vote was granted
      Bool
  | CommandResult
      -- | Leader
      node
      -- | Result
      result
      -- | Request identifier
      RequestId

data RaftSpec entry node result message m = MkRaftSpec
  { _readLogEntry :: LogIndex -> m (Maybe entry),
    _writeLogEntry :: LogIndex -> Term -> entry -> m (),
    _readTerm :: m Term,
    _writeTerm :: Term -> m (),
    _readVotedFor :: m (Maybe node),
    _voteFor :: Maybe node -> m (),
    _applyLogEntry :: entry -> m result,
    _serializeRPC :: RPC node entry -> message,
    _serializeRPCResult :: RPCResult node result -> message,
    _deserializeRPC :: message -> Maybe (RPC node entry),
    _deserializeRPCResult :: message -> Maybe (RPCResult node result),
    _send :: node -> message -> m (),
    _receive :: m message
  }

makeLenses ''RaftSpec

data Role
  = Leader
  | Follower
  | Candidate
  deriving (Eq, Show, Ord, Enum, Bounded)

-- TODO: make some of these fields persistent
data RaftState node entry = MkRaftState
  { _role :: !Role,
    _term :: !Term,
    _votedFor :: !(Maybe node),
    _currentLeader :: !(Maybe node),
    _logEntries :: !(Seq (Term, entry)),
    _commitIndex :: !LogIndex,
    _lastApplied :: !LogIndex
  }

makeLenses ''RaftState

initialRaftState :: RaftState node entry
initialRaftState =
  MkRaftState
    { _role = Follower,
      _term = 1,
      _votedFor = Nothing,
      _currentLeader = Nothing,
      _logEntries = mempty,
      _commitIndex = 0,
      _lastApplied = 0
    }



