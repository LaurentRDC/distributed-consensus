{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Network.Consensus.Raft.Algorithm (server) where

import Control.Concurrent.Class.MonadMVar (MonadMVar)
import Control.Concurrent.Class.MonadSTM (atomically, writeTQueue)
import Control.Monad (forever, unless, when)
import Control.Monad.Class.MonadAsync (MonadAsync)
import Control.Monad.Class.MonadFork (MonadFork, forkIO, labelThisThread)
import Control.Monad.Class.MonadThrow (MonadMask)
import Control.Monad.Class.MonadTimer (MonadDelay)
import Control.Monad.Trans.Class (lift)
import Data.Functor ((<&>))
import Data.Sequence (Seq (..))
import qualified Data.Sequence as Seq
import qualified Data.Set as Set
import Lens.Micro.Platform (use, view, (%=), (+=), (.=), (^.))
import Network.Consensus.Raft.Timer (resetTimer)
import Network.Consensus.Raft.Transformer
  ( Config (..),
    Event (..),
    LogIndex,
    RPC (..),
    RPCResult (RequestVoteResult),
    RaftT,
    RaftTrace (..),
    Role (..),
    Term,
    configuration,
    currentLeader,
    dequeueEvent,
    deserializeRPC,
    deserializeRPCResult,
    electionTimer,
    eventQueue,
    heartBeatTimer,
    lastApplied,
    logEntries,
    nextElectionTimeout,
    quorum,
    receive,
    role,
    sendHeartbeat,
    sendRPCConcurrently,
    sendRPCResult,
    specification,
    term,
    trace,
    tracer,
    updateTerm,
    voteFor,
    votedFor,
    writeTerm,
    yesVotes,
  )

server ::
  ( Ord node,
    MonadMask m,
    MonadFork m,
    MonadMVar m,
    MonadAsync m,
    MonadDelay m
  ) =>
  RaftT entry node result message m ()
server = do
  spec <- view specification
  queue <- view eventQueue
  self <- view configuration <&> nodeId
  _ <-
    lift $
      forkIO $
        let trace' makeTrace = spec ^. tracer $ makeTrace self
         in receiveMessages spec queue trace'

  resetHeartBeatTimer
  resetElectionTimer
  forever (dequeueEvent >>= handleEvent)
  where
    receiveMessages spec queue trace' = do
      labelThisThread "receiveMessages"
      let decodeRPC = spec ^. deserializeRPC
          decodeRPCResult = spec ^. deserializeRPCResult
          recv = spec ^. receive
      forever $ do
        message <- recv
        case decodeRPC message of
          Right rpc -> do
            atomically $ writeTQueue queue (EventRPC rpc)
          Left rpcErrorMessage -> case decodeRPCResult message of
            Right result -> atomically $ writeTQueue queue (EventRPCResult result)
            Left rpcResultErrorMessage ->
              let errMsg = "Failure to deserialize. RPC: " <> rpcErrorMessage <> ", RPCResult: " <> rpcResultErrorMessage
               in trace' (`DeserializationError` errMsg)

handleEvent ::
  ( Ord node,
    MonadDelay m,
    MonadMask m,
    MonadFork m,
    MonadAsync m,
    MonadMVar m
  ) =>
  Event node entry result -> RaftT entry node result message m ()
handleEvent EventElectionTimeout = do
  r <- use role
  when (r /= Leader) $ do
    when (r == Candidate) $ do
      trace SplitElection
    unless (r == Candidate) $ do
      trace ElectionTriggered
    becomeCandidate
handleEvent EventHeartBeatTimeout = sendHeartbeat
handleEvent (EventRPC (RequestVote candidateTerm candidateNode candidateLastLogEntry candidateLastLogEntryTerm)) =
  handleRequestVote candidateTerm candidateNode candidateLastLogEntry candidateLastLogEntryTerm
handleEvent (EventRPC (HeartBeat aeTerm senderNodeId lastLogIndex aeCommitIndedx)) =
  handleHeartBeat aeTerm senderNodeId
handleEvent (EventRPC rpc) = error "rpc"
handleEvent (EventRPCResult (RequestVoteResult voter voterTerm votedForUs)) =
  handleRequestVoteResult voter voterTerm votedForUs
handleEvent (EventRPCResult result) = error "eventRPCResult"

-- | How to handle a term provided by another node. If this term
-- is larger than ours, this means that we must clear some state from
-- the previous term.
--
-- Returns the comparison between the incoming term and our term before this function ran.
-- Therefore, a return value of 'GT' means that the term provided as input was larger than
-- our term.
handleTermNumber ::
  (MonadMVar m) =>
  Term -> RaftT entry node result message m Ordering
handleTermNumber newTerm = do
  (currTerm, _newTerm) <- updateTerm (const newTerm)

  when (newTerm /= currTerm) (votedFor .= Nothing)

  pure $ compare newTerm currTerm

handleRequestVote ::
  ( Eq node,
    MonadMVar m,
    MonadAsync m,
    MonadMask m,
    MonadFork m,
    MonadDelay m
  ) =>
  Term -> node -> LogIndex -> Term -> RaftT entry node result message m ()
handleRequestVote candidateTerm candidateNode candidateLastLogIndex candidateLastLogIndexTerm = do
  _ <- handleTermNumber candidateTerm
  ourTerm <- use term
  trace (\ourTerm' ourNode -> VoteRequestedBy ourTerm' ourNode candidateTerm candidateNode)
  mAlreadyVoted <- use votedFor
  self <- view configuration <&> nodeId
  entries <- use logEntries
  case mAlreadyVoted of
    -- We haven't voted yet
    Nothing ->
      if (candidateLastLogIndex, candidateLastLogIndexTerm) >= lastLogInfo entries
        then do
          votedFor .= Just candidateNode
          grantVote ourTerm
          trace (\ourTerm' ourNode -> VotedFor ourTerm' ourNode candidateTerm candidateNode)
        else do
          sendRPCResult candidateNode (RequestVoteResult self ourTerm False)
    -- We already voted for this candidate
    Just someCandidate
      | someCandidate == candidateNode ->
          sendRPCResult candidateNode (RequestVoteResult self ourTerm True)
    -- We already voted, for another candidate
    Just _ ->
      sendRPCResult candidateNode (RequestVoteResult self ourTerm False)
  where
    lastLogInfo :: Seq (Term, entry) -> (LogIndex, Term)
    lastLogInfo Seq.Empty = (0, 0)
    lastLogInfo entries@(_ Seq.:|> (lastEntryTerm, _)) = (fromIntegral $ Seq.length entries, lastEntryTerm)

    grantVote ourTerm = do
      self <- view configuration <&> nodeId
      sendRPCResult candidateNode (RequestVoteResult self ourTerm True)
      r <- use role
      when (r == Follower) resetElectionTimer

handleHeartBeat ::
  ( MonadMVar m,
    MonadAsync m,
    MonadMask m,
    MonadFork m,
    MonadDelay m
  ) =>
  Term -> node -> RaftT entry node result message m ()
handleHeartBeat aeTerm senderNodeId =
  handleTermNumber aeTerm >>= \case
    GT -> becomeFollower senderNodeId >> resetElectionTimer
    EQ -> becomeFollower senderNodeId >> resetElectionTimer
    LT -> pure ()

handleRequestVoteResult ::
  ( Ord node,
    MonadAsync m,
    MonadMask m,
    MonadFork m,
    MonadMVar m,
    MonadDelay m
  ) =>
  node -> Term -> Bool -> RaftT entry node result message m ()
handleRequestVoteResult voter voterTerm votedForUs = do
  handleTermNumber voterTerm >>= \case
    LT -> pure () -- Vote request for an old term
    _ -> do
      ourRole <- use role
      when (ourRole == Candidate) $
        if votedForUs
          then do
            trace (\ourTerm ourNode -> VoteGrantedFrom ourTerm ourNode voter)
            yesVotes %= Set.insert voter
            checkElection
          else
            trace (\ourTerm ourNode -> VoteDeniedFrom ourTerm ourNode voter)

becomeCandidate ::
  ( MonadAsync m,
    MonadMask m,
    MonadFork m,
    MonadMVar m,
    MonadDelay m
  ) =>
  RaftT entry node result message m ()
becomeCandidate = do
  trace BecameCandidate
  role .= Candidate

  term += 1
  w <- view (specification . writeTerm)
  thisTerm <- use term
  lift (w thisTerm)

  self <- view configuration <&> nodeId
  v <- view (specification . voteFor)
  lift $ v (Just self)
  trace (VotedFor thisTerm self)
  votedFor .= Just self
  yesVotes .= Set.singleton self

  resetElectionTimer

  checkElection -- there might only be a single node in the cluster
  r <- use role
  when (r == Candidate) $ do
    peers <- view configuration <&> otherNodes
    currentTerm <- use term
    lastLogIndex <- use lastApplied
    lastLogTerm <-
      use logEntries >>= \case
        Empty -> pure 0
        (_ :|> (lastTerm, _)) -> pure lastTerm
    let rpc = RequestVote currentTerm self lastLogIndex lastLogTerm
    sendRPCConcurrently peers rpc

checkElection ::
  ( MonadAsync m,
    MonadMask m,
    MonadFork m,
    MonadMVar m,
    MonadDelay m
  ) =>
  RaftT entry node result message m ()
checkElection = do
  numYes <- Set.size <$> use yesVotes
  q <- quorum
  when (numYes >= q) becomeLeader

becomeLeader ::
  ( MonadDelay m,
    MonadMVar m,
    MonadFork m,
    MonadMask m,
    MonadAsync m
  ) =>
  RaftT entry node result message m ()
becomeLeader = do
  role .= Leader
  self <- view configuration <&> nodeId
  currentLeader .= Just self
  yesVotes .= Set.empty
  trace LeaderElected

  -- TODO: send append all entries messages to all followers
  sendHeartbeat -- Note that 'sendHeartbeat' will also reset the heartbeat timer

becomeFollower ::
  (MonadMVar m) =>
  -- | Leader node ID
  node ->
  RaftT entry node result message m ()
becomeFollower leaderNodeId = do
  r <- use role
  unless (r == Follower) $ do
    trace BecameFollower
    role .= Follower
  currentLeader .= Just leaderNodeId

resetHeartBeatTimer ::
  ( MonadAsync m,
    MonadMask m,
    MonadFork m,
    MonadMVar m,
    MonadDelay m
  ) =>
  RaftT entry node result message m ()
resetHeartBeatTimer = do
  config <- view configuration
  view heartBeatTimer >>= lift . resetTimer config.heartBeatTimeout

resetElectionTimer ::
  ( MonadAsync m,
    MonadMask m,
    MonadFork m,
    MonadMVar m,
    MonadDelay m
  ) =>
  RaftT entry node result message m ()
resetElectionTimer = do
  electionTimeout <- nextElectionTimeout
  view electionTimer >>= lift . resetTimer electionTimeout
