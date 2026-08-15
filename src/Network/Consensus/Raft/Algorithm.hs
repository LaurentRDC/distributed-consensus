{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Network.Consensus.Raft.Algorithm
  ( server,
  )
where

import Control.Concurrent.Class.MonadMVar (MonadMVar, putMVar, tryTakeMVar)
import Control.Concurrent.Class.MonadSTM (MonadSTM, atomically, writeTQueue)
import Control.Monad (forever, unless, when)
import Control.Monad.Class.MonadAsync (MonadAsync, async, cancel, link)
import Control.Monad.Class.MonadFork (MonadFork, labelThisThread)
import Control.Monad.Class.MonadThrow (MonadMask)
import Control.Monad.Class.MonadTimer (MonadDelay, threadDelay)
import Control.Monad.Trans.Class (lift)
import Data.Foldable (for_)
import Data.Functor ((<&>))
import qualified Data.Map.Strict as Map
import qualified Data.Sequence as Seq
import qualified Data.Set as Set
import qualified Data.Vector as Vector
import Lens.Micro.Platform (at, use, view, (%=), (.=), (^.))
import Network.Consensus.Raft.Client (Request (..), Response (..))
import Network.Consensus.Raft.Domain
import Network.Consensus.Raft.Log (LogIndex, Lookup (..), Snapshot (Snapshot, sMetadata), SnapshotMetadata (..), lastLogIndex, (!?))
import qualified Network.Consensus.Raft.Log as Log
import Network.Consensus.Raft.Transformer
  ( AppendEntries (..),
    AppendEntriesResult (..),
    ClusterMembershipRequest (..),
    ClusterMembershipResult (..),
    Command (..),
    Config (..),
    Event (..),
    InstallSnapshot (..),
    InstallSnapshotResult (..),
    LogEntry (..),
    RPC (..),
    RPCResult (..),
    RaftT,
    RaftTrace (..),
    acceptClientRequest,
    appendLogEntry,
    applyLogEntries,
    applySnapshot,
    becomeCandidate,
    becomeFollower,
    checkElection,
    clusterConfiguration,
    clusterNodes,
    commandLog,
    commitIndex,
    configuration,
    currentLeader,
    currentSnapshot,
    dequeueEvent,
    eventQueue,
    exitLock,
    matchIndex,
    nextIndex,
    peers,
    receiveAdminCommand,
    receiveClientRequest,
    receiveRPC,
    receiveRPCResult,
    resetElectionTimer,
    resetHeartBeatTimer,
    role,
    self,
    sendAppendEntriesTo,
    sendClientResponse,
    sendHeartbeat,
    sendRPC,
    sendRPCResult,
    specification,
    term,
    trace,
    tracer,
    updateTerm,
    votedFor,
    whenRole,
    yesVotes,
  )
import Network.Consensus.Raft.Transformer.Spec (AdminCommand (..), ClusterMembershipError (..))

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
  s <- self

  let trace' makeTrace = spec ^. tracer $ makeTrace s
  t1 <- lift $ async $ receiveRPCs spec queue trace'
  t2 <- lift $ async $ receiveRPCResults spec queue trace'
  t3 <- lift $ async $ receiveClientRequests spec queue trace'
  t4 <- lift $ async $ receiveAdminCommands spec queue trace'
  let receiveLoops = [t1, t2, t3, t4]
  for_ receiveLoops (lift . link)

  -- There are some initialization steps which require the configuration.
  -- Instead of coupling 'initialRaftState' with the configuration,
  -- we perform some initialization here.
  ps <- peers
  nextIndex .= Map.fromSet (const 0) ps

  -- NonMembers don't participate in elections, but also don't hold elections
  -- for themselves. This prevents a NonMember from electing itself as the leader
  -- of its own trivial cluster
  r <- use role
  unless (r == NonMember) $ do
    resetHeartBeatTimer
    resetElectionTimer

  lock <- view exitLock
  let loop = do
        lift (tryTakeMVar lock) >>= \case
          Nothing -> do
            ev <- dequeueEvent
            trace (\t n -> EventReceived t n ev)
            handleEvent ev
            loop
          Just () -> pure ()

  loop

  for_ receiveLoops (lift . cancel)
  trace GracefulShutdown
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
    receiveAdminCommands spec queue trace' = do
      labelThisThread "receiveAdminCommands"
      let recv = spec ^. receiveAdminCommand
      forever $ do
        recv >>= \case
          Left errMsg -> trace' (`DeserializationError` errMsg)
          Right request -> atomically $ writeTQueue queue (EventAdminCommand request)

handleEvent ::
  ( Ord node,
    MonadDelay m,
    MonadMask m,
    MonadFork m,
    MonadAsync m,
    MonadMVar m
  ) =>
  Event node entry result state -> RaftT entry node state result m ()
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
handleEvent (EventRPC (IS installSnapshot)) = handleInstallSnapshot installSnapshot
handleEvent (EventRPC (CM clusterMembershipRequest)) = handleClusterMembershipRequest clusterMembershipRequest
handleEvent (EventRPCResult (RequestVoteResult voter voterTerm votedForUs)) =
  handleRequestVoteResult voter voterTerm votedForUs
handleEvent (EventRPCResult (AER appendEntriesResult)) =
  handleAppendEntriesResult appendEntriesResult
handleEvent (EventRPCResult (ISR installSnapshotResult)) =
  handleInstallSnapshotResult installSnapshotResult
handleEvent (EventRPCResult (CMR clusterMembershipResult)) = handleClusterMembershipResult clusterMembershipResult
handleEvent (EventAdminCommand adminCommand) = handleAdminCommand adminCommand

handleClientRequest ::
  (Ord node, MonadMVar m, MonadAsync m) =>
  Request node entry -> RaftT entry node state result m ()
handleClientRequest (MkRequest clientId entry) =
  use role >>= \case
    NonMember ->
      sendClientResponse clientId (NotLeader Nothing)
    Candidate ->
      sendClientResponse clientId (NotLeader Nothing)
    Follower ->
      use currentLeader >>= sendClientResponse clientId . NotLeader
    Leader -> do
      reqId <- acceptClientRequest clientId
      let command = Command entry reqId
      trace (\t n -> CommandReceived t n command)
      appendLogEntry (LogEntryCommand command)

handleAppendEntries ::
  ( Ord node,
    MonadMVar m,
    MonadAsync m,
    MonadMask m,
    MonadFork m,
    MonadDelay m
  ) =>
  AppendEntries node entry ->
  RaftT entry node state result m ()
handleAppendEntries (AppendEntries leaderTerm leaderNode prevLogIndex previousLogTerm newEntries leaderCommitIndex) = do
  termComparison <- handleTermNumber leaderTerm

  whenRole Follower $ do
    -- TODO: I believe the next line is redundant because we have a separate
    -- heartbeat mechanism.
    when (termComparison == EQ) resetElectionTimer
    currentLeader .= Just leaderNode

    -- Consistency check
    entries <- use commandLog
    let Snapshot (SnapshotMetadata lastIx _) _ _ = Log.lSnapshot entries
        logIsConsistent =
          case entries !? prevLogIndex of
            NotFound -> prevLogIndex == 0
            LogIndexInSnapshot _ ->
              -- By snapshot construction, indices
              prevLogIndex <= lastIx
            Found (t, _) -> t == previousLogTerm

    let oldLastEntry = lastLogIndex entries - 1
        newLastEntry = prevLogIndex + fromIntegral (length newEntries)

    ourTerm <- use term
    s <- self
    if leaderTerm < ourTerm || not logIsConsistent
      then sendRPCResult leaderNode (AER (AppendEntriesResult ourTerm s False oldLastEntry))
      else do
        commandLog
          %= (`Log.extend` (Seq.fromList $ Vector.toList newEntries))
          . (`Log.keepEntriesUpTo` succ prevLogIndex)
        sendRPCResult leaderNode (AER (AppendEntriesResult ourTerm s True newLastEntry))
        ourCommitIndex <- use commitIndex
        when (leaderCommitIndex > ourCommitIndex) $ do
          commitIndex .= min leaderCommitIndex newLastEntry
          applyLogEntries

handleAppendEntriesResult ::
  (Ord node, MonadMVar m, MonadSTM m) =>
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
  s <- self
  entries <- use commandLog
  case mAlreadyVoted of
    -- We haven't voted yet
    Nothing ->
      if (candidateLastLogIndex, candidateLastLogIndexTerm) >= Log.lastLogInfo entries
        then do
          votedFor .= Just candidateNode
          grantVote ourTerm
          trace (\ourTerm' ourNode -> VotedFor ourTerm' ourNode candidateTerm candidateNode)
        else do
          sendRPCResult candidateNode (RequestVoteResult s ourTerm False)
    -- We already voted for this candidate
    Just someCandidate
      | someCandidate == candidateNode ->
          sendRPCResult candidateNode (RequestVoteResult s ourTerm True)
    -- We already voted, for another candidate
    Just _ ->
      sendRPCResult candidateNode (RequestVoteResult s ourTerm False)
  where
    grantVote ourTerm = do
      s <- self
      sendRPCResult candidateNode (RequestVoteResult s ourTerm True)
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

handleInstallSnapshot ::
  (MonadMVar m) =>
  InstallSnapshot node state -> RaftT entry node state result m ()
handleInstallSnapshot (InstallSnapshot leaderTerm leaderNode snapshot) =
  handleTermNumber leaderTerm >>= \case
    LT ->
      -- old term
      pure () -- TODO: what do I do here?
    _ -> whenRole Follower $ do
      applySnapshot snapshot
      InstallSnapshotResult
        <$> use term
        <*> (view configuration <&> nodeId)
        <*> pure (sMetadata snapshot)
        >>= sendRPCResult leaderNode . ISR

handleInstallSnapshotResult ::
  ( Ord node,
    MonadMVar m
  ) =>
  InstallSnapshotResult node -> RaftT entry node state result m ()
handleInstallSnapshotResult (InstallSnapshotResult responderTerm responderNode (SnapshotMetadata lastIncludedIndex _)) = do
  termComp <- handleTermNumber responderTerm
  ourRole <- use role
  when (ourRole == Leader && termComp == EQ) $ do
    matchIndex . at responderNode .= Just lastIncludedIndex
    nextIndex . at responderNode .= Just (succ lastIncludedIndex)
    -- The only reason to send an 'InstallSnapshot' RPC
    -- (and thus get a result) is because a node has gotten behind
    -- in its entries. With the snapshot installed, we're free to
    -- send the rest of the logs
    sendAppendEntriesTo responderNode

handleClusterMembershipRequest :: (Ord node, MonadMVar m, MonadSTM m) => ClusterMembershipRequest node -> RaftT entry node state result m ()
handleClusterMembershipRequest request = do
  let (requester, resultCtor) = case request of
        ClusterMembershipJoinRequest r -> (r, ClusterMembershipJoinResult)
        ClusterMembershipLeaveRequest r -> (r, ClusterMembershipLeaveResult)
  use role >>= \case
    NonMember -> self >>= \s -> sendRPCResult requester (CMR (resultCtor (Left (NoKnownLeader s))))
    Candidate -> self >>= \s -> sendRPCResult requester (CMR (resultCtor (Left (NoKnownLeader s))))
    Follower ->
      use currentLeader >>= \case
        Nothing -> do
          s <- self
          sendRPCResult requester (CMR (resultCtor (Left (NoKnownLeader s))))
        Just leader -> sendRPC leader (CM request) -- forward to leader
    Leader -> do
      use clusterConfiguration >>= \case
        -- By design, we cannot handle more than one membership request
        -- at a time. The requester must retry later.
        Joint {} -> self >>= \s -> sendRPCResult requester (CMR (resultCtor (Left (OngoingClusterMembershipChange s))))
        Simple {} -> pure ()

      cluster <- clusterNodes
      trace MembershipChangeInitiated
      self >>= \s -> sendRPCResult requester (CMR (resultCtor (Right s)))

      -- By definition, a node that just joined
      -- needs to catch up with a snapshot.
      case request of
        ClusterMembershipJoinRequest _ -> do
          ( InstallSnapshot
              <$> use term
              <*> self
              <*> currentSnapshot
            )
            >>= sendRPC requester . IS
          appendLogEntry
            ( LogEntryMembershipChange
                (Joint cluster (Set.insert requester cluster))
            )
        ClusterMembershipLeaveRequest _ ->
          appendLogEntry
            ( LogEntryMembershipChange
                (Joint cluster (Set.delete requester cluster))
            )

handleClusterMembershipResult ::
  ( MonadDelay m,
    MonadMVar m,
    MonadFork m,
    MonadMask m,
    MonadAsync m
  ) =>
  ClusterMembershipResult node -> RaftT entry node state result m ()
handleClusterMembershipResult = \case
  ClusterMembershipJoinResult (Right leaderId) -> do
    becomeFollower leaderId
    trace JoinedCluster -- TODO: is this the right moment to trace this? Probably should wait until log is replicated
  ClusterMembershipJoinResult (Left err) -> do
    -- The natural delay to wait is one heart beat timeout. What else would we use?
    view configuration <&> heartBeatTimeout >>= lift . threadDelay . fromIntegral
    self >>= \s -> sendRPC (clusterMembershipErrorPeer err) (CM (ClusterMembershipJoinRequest s))
  ClusterMembershipLeaveResult (Right _) -> pure () -- Nothing to do until log entry is applied
  ClusterMembershipLeaveResult (Left err) -> do
    -- The natural delay to wait is one heart beat timeout. What else would we use?
    view configuration <&> heartBeatTimeout >>= lift . threadDelay . fromIntegral
    self >>= \s -> sendRPC (clusterMembershipErrorPeer err) (CM (ClusterMembershipLeaveRequest s))
  where
    clusterMembershipErrorPeer (NoKnownLeader n) = n
    clusterMembershipErrorPeer (OngoingClusterMembershipChange n) = n

handleAdminCommand :: (MonadMVar m) => AdminCommand node -> RaftT entry node state result m ()
handleAdminCommand comm = do
  trace (\t n -> AdminCommandReceived t n comm)
  case comm of
    (ShutDown _requester) ->
      view exitLock >>= lift . (`putMVar` ()) -- TODO: reply to requester
    (JoinCluster _requester target) ->
      self >>= sendRPC target . CM . ClusterMembershipJoinRequest -- TODO: reply to requester
    (LeaveCluster _requester) ->
      use currentLeader
        >>= \case
          Nothing -> pure () -- TODO: reply to admin
          Just leaderId ->
            self >>= sendRPC leaderId . CM . ClusterMembershipLeaveRequest -- TODO: reply to requester
