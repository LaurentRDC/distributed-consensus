{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Network.Consensus.Raft.Algorithm (server) where

import Control.Arrow ((&&&))
import Control.Concurrent.Class.MonadMVar (MonadMVar)
import Control.Concurrent.Class.MonadSTM (atomically, writeTQueue)
import Control.Monad (forever, unless, when, (<=<))
import Control.Monad.Class.MonadAsync (MonadAsync, async, link)
import Control.Monad.Class.MonadFork (MonadFork, labelThisThread)
import Control.Monad.Class.MonadThrow (MonadMask)
import Control.Monad.Class.MonadTimer (MonadDelay)
import Control.Monad.Trans.Class (lift)
import Data.Foldable (traverse_)
import Data.Functor ((<&>))
import qualified Data.Map.Strict as Map
import Data.Sequence (Seq (..), (|>))
import qualified Data.Sequence as Seq
import qualified Data.Set as Set
import qualified Data.Vector as Vector
import Lens.Micro.Platform (at, use, view, (%=), (+=), (.=), (^.))
import Network.Consensus.Raft.Client (Request (..), Response (..))
import Network.Consensus.Raft.Timer (resetTimer)
import Network.Consensus.Raft.Transformer
  ( AppendEntries (..),
    AppendEntriesResult (..),
    Command (..),
    Config (..),
    Event (..),
    LogIndex,
    RPC (..),
    RPCResult (..),
    RaftT,
    RaftTrace (..),
    Role (..),
    Term,
    acceptClientRequest,
    applyLogEntries,
    commitIndex,
    configuration,
    currentLeader,
    dequeueEvent,
    electionTimer,
    eventQueue,
    heartBeatTimer,
    lastApplied,
    logEntries,
    matchIndex,
    nextElectionTimeout,
    nextIndex,
    quorum,
    receiveClientRequest,
    receiveRPC,
    receiveRPCResult,
    role,
    sendAppendEntriesTo,
    sendClientResponse,
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
  RaftT entry node state result m ()
server = do
  spec <- view specification
  queue <- view eventQueue
  self <- view configuration <&> nodeId

  let trace' makeTrace = spec ^. tracer $ makeTrace self
  _ <- lift $ link <=< async $ receiveRPCs spec queue trace'
  _ <- lift $ link <=< async $ receiveRPCResults spec queue trace'
  _ <- lift $ link <=< async $ receiveClientRequests spec queue trace'

  -- There are some initialization steps which require the configuration.
  -- Instead of coupling 'initialRaftState' with the configuration,
  -- we perform some initialization here.
  peers <- view configuration <&> otherNodes
  nextIndex .= Map.fromSet (const 0) peers

  resetHeartBeatTimer
  resetElectionTimer
  forever $ do
    ev <- dequeueEvent
    trace (\t n -> EventReceived t n ev)
    handleEvent ev
  where
    receiveRPCs spec queue trace' = do
      labelThisThread "receiceRPCs"
      let recv = spec ^. receiveRPC
      forever $ do
        recv >>= \case
          Left errMsg -> trace' (`DeserializationError` errMsg)
          Right rpc -> atomically $ writeTQueue queue (EventRPC rpc)
    receiveRPCResults spec queue trace' = do
      labelThisThread "receiceRPCResults"
      let recv = spec ^. receiveRPCResult
      forever $ do
        recv >>= \case
          Left errMsg -> trace' (`DeserializationError` errMsg)
          Right rpcResult -> atomically $ writeTQueue queue (EventRPCResult rpcResult)
    receiveClientRequests spec queue trace' = do
      labelThisThread "receiceClientRequests"
      let recv = spec ^. receiveClientRequest
      forever $ do
        recv >>= \case
          Left errMsg -> trace' (`DeserializationError` errMsg)
          Right request -> atomically $ writeTQueue queue (EventIncomingClientRequest request)

handleEvent ::
  ( Ord node,
    MonadDelay m,
    MonadMask m,
    MonadFork m,
    MonadAsync m,
    MonadMVar m
  ) =>
  Event node entry result -> RaftT entry node state result m ()
handleEvent EventElectionTimeout = do
  r <- use role
  when (r /= Leader) $ do
    when (r == Candidate) $ do
      trace SplitElection
    unless (r == Candidate) $ do
      trace ElectionTriggered
    becomeCandidate
handleEvent EventHeartBeatTimeout = sendHeartbeat
handleEvent (EventIncomingClientRequest request) = handleClientRequest request
handleEvent (EventRPC (RequestVote candidateTerm candidateNode candidateLastLogEntry candidateLastLogEntryTerm)) =
  handleRequestVote candidateTerm candidateNode candidateLastLogEntry candidateLastLogEntryTerm
handleEvent (EventRPC (HeartBeat aeTerm senderNodeId _lastLogIndex _aeCommitIndex)) =
  handleHeartBeat aeTerm senderNodeId
handleEvent (EventRPC (AE appendEntries)) = handleAppendEntries appendEntries
handleEvent (EventRPCResult (RequestVoteResult voter voterTerm votedForUs)) =
  handleRequestVoteResult voter voterTerm votedForUs
handleEvent (EventRPCResult (AER appendEntriesResult)) =
  handleAppendEntriesResult appendEntriesResult
handleEvent (EventRPCResult (ClientRequestResult _commandResponse)) = pure () -- only meant for clients to handle

handleClientRequest ::
  (Ord node, MonadFork m, MonadMVar m) =>
  Request node entry -> RaftT entry node state result m ()
handleClientRequest (MkRequest clientId entry) =
  use role >>= \case
    Candidate ->
      sendClientResponse clientId (NotLeader Nothing)
    Follower ->
      use currentLeader >>= sendClientResponse clientId . NotLeader
    Leader -> do
      reqId <- acceptClientRequest clientId
      let command = MkCommand entry reqId
      trace (\t n -> CommandReceived t n command)
      ourTerm <- use term
      logEntries %= (|> (ourTerm, command))
      peers <- view configuration <&> otherNodes
      -- TODO: sendAppendEntriesTo in parallel
      traverse_ sendAppendEntriesTo peers

      -- In a single-node cluster, we would already have
      -- quorum to update our commit index
      applyLogEntries

handleAppendEntries ::
  ( MonadMVar m,
    MonadAsync m,
    MonadMask m,
    MonadFork m,
    MonadDelay m
  ) =>
  AppendEntries node entry ->
  RaftT entry node state result m ()
handleAppendEntries (AppendEntries leaderTerm leaderNode prevLogIndex previousLogTerm newEntries leaderCommitIndex) = do
  termComparison <- handleTermNumber leaderTerm

  use role >>= \case
    Leader -> pure ()
    Candidate -> pure ()
    Follower -> do
      -- TODO: I believe the next line is redundant because we have a separate
      -- heartbeat mechanism.
      when (termComparison == EQ) resetElectionTimer
      currentLeader .= Just leaderNode

      -- Consistency check
      entries <- use logEntries
      let logIsConsistent = case entries Seq.!? fromIntegral prevLogIndex of
            Nothing -> prevLogIndex == 0
            Just (t, _) -> t == previousLogTerm

      let oldLastEntry = fromIntegral $ Seq.length entries - 1
          newLastEntry = prevLogIndex + fromIntegral (length newEntries)

      ourTerm <- use term
      self <- view configuration <&> nodeId
      if leaderTerm < ourTerm || not logIsConsistent
        then sendRPCResult leaderNode (AER (AppendEntriesResult ourTerm self False oldLastEntry))
        else do
          logEntries
            %= (Seq.>< (Seq.fromList $ Vector.toList newEntries))
            . Seq.take (fromIntegral $ succ prevLogIndex)
          sendRPCResult leaderNode (AER (AppendEntriesResult ourTerm self True newLastEntry))
          ourCommitIndex <- use commitIndex
          when (leaderCommitIndex > ourCommitIndex) $ do
            commitIndex .= min leaderCommitIndex newLastEntry
            applyLogEntries

handleAppendEntriesResult ::
  (Ord node, MonadMVar m) =>
  AppendEntriesResult node result ->
  RaftT entry node state result m ()
handleAppendEntriesResult (AppendEntriesResult responderTerm responderNode responderSuccess responderLogIndex) = do
  termComp <- handleTermNumber responderTerm
  ourRole <- use role
  when (ourRole == Leader && termComp == EQ) $
    if responderSuccess
      then do
        matchIndex . at responderNode .= Just responderLogIndex
        nextIndex . at responderNode .= Just (succ responderLogIndex)
        applyLogEntries
      else do
        nextIndex %= Map.adjust pred responderNode
        sendAppendEntriesTo responderNode -- Retry

-- | How to handle a term provided by another node. If this term
-- is larger than ours, this means that we must clear some state from
-- the previous term.
--
-- Returns the comparison between the incoming term and our term before this function ran.
-- Therefore, a return value of 'GT' means that the term provided as input was larger than
-- our term.
handleTermNumber ::
  (MonadMVar m) =>
  Term -> RaftT entry node state result m Ordering
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
  Term ->
  node ->
  LogIndex ->
  Term ->
  RaftT entry node state result m ()
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
  Term -> node -> RaftT entry node state result m ()
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
  node -> Term -> Bool -> RaftT entry node state result m ()
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
  ( Ord node,
    MonadAsync m,
    MonadMask m,
    MonadFork m,
    MonadMVar m,
    MonadDelay m
  ) =>
  RaftT entry node state result m ()
becomeCandidate = do
  trace BecameCandidate
  role .= Candidate
  -- The following pieces of state are only useful to leaders
  nextIndex .= mempty
  matchIndex .= mempty

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
  RaftT entry node state result m ()
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
  RaftT entry node state result m ()
becomeLeader = do
  role .= Leader
  (self, peers) <- view configuration <&> (nodeId &&& otherNodes)
  currentLeader .= Just self
  yesVotes .= Set.empty

  lastLogIndex <- use logEntries <&> Seq.length
  nextIndex .= Map.fromSet (const (fromIntegral lastLogIndex + 1)) peers

  trace LeaderElected

  -- TODO: send append all entries messages to all followers
  sendHeartbeat -- Note that 'sendHeartbeat' will also reset the heartbeat timer

becomeFollower ::
  (MonadMVar m) =>
  -- | Leader node ID
  node ->
  RaftT entry node state result m ()
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
  RaftT entry node state result m ()
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
  RaftT entry node state result m ()
resetElectionTimer = do
  electionTimeout <- nextElectionTimeout
  view electionTimer >>= lift . resetTimer electionTimeout
