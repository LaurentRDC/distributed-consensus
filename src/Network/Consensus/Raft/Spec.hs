{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE ExplicitForAll #-}
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
    tracer,

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
    yesVotes,
    randomGen,

    -- * Types
    RPC (..),
    RPCResult (..),
    Role (..),
    LogIndex,
    Term,
    RaftTrace (..),
  )
where

import Data.Int (Int64)
import Data.Sequence (Seq)
import Data.Set (Set)
import Data.Vector (Vector)
import Data.Word (Word64)
import Lens.Micro.Platform (makeLenses)
import System.Random (StdGen, mkStdGen64)

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
      -- | Voter node
      node
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

data RaftTrace node
  = LeaderElected node
  deriving (Eq, Ord, Show)

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
    _receive :: m message,
    _tracer :: RaftTrace node -> m ()
  }

makeLenses ''RaftSpec

data Role
  = Leader
  | Follower
  | Candidate
  deriving (Eq, Show, Ord, Enum, Bounded)

data RaftState node entry = MkRaftState
  { _role :: !Role,
    _term :: !Term,
    _votedFor :: !(Maybe node),
    _currentLeader :: !(Maybe node),
    _logEntries :: !(Seq (Term, entry)),
    _commitIndex :: !LogIndex,
    _lastApplied :: !LogIndex,
    -- | Set of votes received in the current term
    _yesVotes :: !(Set node),
    _randomGen :: !StdGen
  }

makeLenses ''RaftState

initialRaftState :: (Ord node) => Word64 -> RaftState node entry
initialRaftState seed =
  MkRaftState
    { _role = Follower,
      _term = 1,
      _votedFor = Nothing,
      _currentLeader = Nothing,
      _logEntries = mempty,
      _commitIndex = 0,
      _lastApplied = 0,
      _yesVotes = mempty,
      -- We use mkStdGen64 for reproducibility across 32-bit and 64-bit architectures
      _randomGen = mkStdGen64 seed
    }
