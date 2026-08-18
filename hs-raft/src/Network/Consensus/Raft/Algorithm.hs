{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Network.Consensus.Raft.Algorithm
  ( server,
  )
where

import Control.Concurrent.Class.MonadMVar (MonadMVar, putMVar)
import Control.Concurrent.Class.MonadSTM (atomically, writeTQueue)
import Control.Monad (forever, unless, when)
import Control.Monad.Class.MonadAsync (MonadAsync, async, cancel, link)
import Control.Monad.Class.MonadFork (MonadFork, labelThisThread)
import Control.Monad.Class.MonadThrow (MonadMask)
import Control.Monad.Class.MonadTimer (MonadDelay, threadDelay)
import Control.Monad.Trans.Class (lift)
import Data.Foldable (for_)
import Data.Functor ((<&>))
import Data.List.NonEmpty (NonEmpty)
import qualified Data.List.NonEmpty as NonEmpty
import qualified Data.Map.Strict as Map
import qualified Data.Set as Set
import Lens.Micro.Platform (at, use, view, (%=), (.=), (^.))
import Network.Consensus.Raft.Admin (AdminCommand (..), AdminRequest)
import qualified Network.Consensus.Raft.Admin as Admin
import Network.Consensus.Raft.Client (ClientRequest, ClientResult (..))
import Network.Consensus.Raft.Domain
import Network.Consensus.Raft.Log (Lookup (..), lastLogIndex, (!?))
import qualified Network.Consensus.Raft.Log as Log
import Network.Consensus.Raft.Messaging (Request (..), Response (..))
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
    acceptClientRequests,
    appendLogEntries,
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
    persistSnapshot,
    receiveAdminRequest,
    receiveClientRequests,
    receiveRPC,
    receiveRPCResult,
    resetElectionTimer,
    resetHeartBeatTimer,
    role,
    self,
    sendAdminResponse,
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
import Network.Consensus.Raft.Transformer.Spec (ClusterMembershipError (..))

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
  t1 <- lift $ async $ receiveRPCsThread spec queue trace'
  t2 <- lift $ async $ receiveRPCResultsThread spec queue trace'
  t3 <- lift $ async $ receiveClientRequestsThread spec queue
  t4 <- lift $ async $ receiveAdminRequestsThread spec queue trace'
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

  let loop = do
        dequeueEvent >>= \case
          Nothing -> pure () -- shutdown
          Just ev -> do
            trace (`EventReceived` ev)
            handleEvent ev
            loop

  loop

  for_ receiveLoops (lift . cancel)
  trace GracefulShutdown
  where
    receiveRPCsThread spec queue trace' = do
      labelThisThread "receiceRPCs"
      let recv = spec ^. receiveRPC
      forever $ do
        recv >>= \case
          Left errMsg -> trace' (`DeserializationError` errMsg)
          Right rpc -> atomically $ writeTQueue queue (EventRPC rpc)
    receiveRPCResultsThread spec queue trace' = do
      labelThisThread "receiceRPCResults"
      let recv = spec ^. receiveRPCResult
      forever $ do
        recv >>= \case
          Left errMsg -> trace' (`DeserializationError` errMsg)
          Right rpcResult -> atomically $ writeTQueue queue (EventRPCResult rpcResult)
    receiveClientRequestsThread spec queue = do
      labelThisThread "receiceClientRequests"
      let recv = spec ^. receiveClientRequests
      forever $ recv >>= atomically . writeTQueue queue . EventIncomingClientRequest
    receiveAdminRequestsThread spec queue trace' = do
      labelThisThread "receiveAdminRequests"
      let recv = spec ^. receiveAdminRequest
      forever $ do
        recv >>= \case
          Left errMsg -> trace' (`DeserializationError` errMsg)
          Right request -> atomically $ writeTQueue queue (EventAdminRequest request)

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
handleEvent (EventIncomingClientRequest requests) = handleClientRequest requests
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
handleEvent (EventAdminRequest adminCommand) = handleAdminRequest adminCommand
handleEvent (EventSnapshotPersisted snapshot) = applySnapshot snapshot

handleClientRequest ::
  (Ord node, MonadMVar m, MonadAsync m) =>
  NonEmpty (ClientRequest node entry) -> RaftT entry node state result m ()
handleClientRequest requests = do
  mLeaderId <- use currentLeader
  use role >>= \case
    NonMember -> do
      for_ requests $ \(MkRequest reqId clientId _) ->
        -- TODO: reply in parallel
        sendClientResponse clientId (MkResponse reqId mLeaderId NotLeader)
    Candidate ->
      for_ requests $ \(MkRequest reqId clientId _) ->
        -- TODO: reply in parallel
        sendClientResponse clientId (MkResponse reqId mLeaderId NotLeader)
    Follower ->
      for_ requests $ \(MkRequest reqId clientId _) ->
        sendClientResponse clientId (MkResponse reqId mLeaderId NotLeader)
    Leader -> do
      reqIds <- acceptClientRequests requests
      let commands = NonEmpty.zipWith Command reqIds (requestPayload <$> requests)
      -- TODO: trace in separate thread?
      for_ commands $ \(Command rid entry) ->
        trace (\ctx -> CommandReceived ctx rid entry)
      appendLogEntries $ LogEntryCommand <$> commands

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
    let (SnapshotMetadata lastIx _) = Log.lSnapshotMetadata entries
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
          %= (`Log.extend` newEntries)
          . (`Log.keepEntriesUpTo` succ prevLogIndex)
        sendRPCResult leaderNode (AER (AppendEntriesResult ourTerm s True newLastEntry))
        ourCommitIndex <- use commitIndex
        when (leaderCommitIndex > ourCommitIndex) $ do
          commitIndex .= min leaderCommitIndex newLastEntry
          applyLogEntries

handleAppendEntriesResult ::
  (Ord node, MonadMVar m, MonadAsync m) =>
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
  trace (\ctx -> VoteRequestedBy ctx candidateTerm candidateNode)
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
          trace (\ctx -> VotedFor ctx candidateTerm candidateNode)
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
            trace (`VoteGrantedFrom` voter)
            yesVotes %= Set.insert voter
            checkElection
          else
            trace (`VoteDeniedFrom` voter)

handleInstallSnapshot ::
  (MonadMVar m, MonadAsync m) =>
  InstallSnapshot node state -> RaftT entry node state result m ()
handleInstallSnapshot (InstallSnapshot leaderTerm leaderNode snapshot) =
  handleTermNumber leaderTerm >>= \case
    LT ->
      -- old term
      pure () -- TODO: what do I do here?
    _ -> whenRole Follower $ do
      persistSnapshot snapshot
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

handleClusterMembershipRequest :: (Ord node, MonadMVar m, MonadAsync m) => ClusterMembershipRequest node -> RaftT entry node state result m ()
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
          appendLogEntries
            ( NonEmpty.singleton
                ( LogEntryMembershipChange
                    (Joint cluster (Set.insert requester cluster))
                )
            )
        ClusterMembershipLeaveRequest _ ->
          appendLogEntries
            ( NonEmpty.singleton
                ( LogEntryMembershipChange
                    (Joint cluster (Set.delete requester cluster))
                )
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

handleAdminRequest :: (MonadMVar m) => AdminRequest node -> RaftT entry node state result m ()
handleAdminRequest req@(MkRequest reqId adminId command) = do
  trace (`AdminRequestReceived` req)
  mLeaderId <- use currentLeader
  case command of
    ShutDown -> do
      view exitLock >>= lift . (`putMVar` ())
      sendAdminResponse adminId (MkResponse reqId mLeaderId Admin.ShutdownInitiated)
    (JoinCluster target) -> do
      self >>= sendRPC target . CM . ClusterMembershipJoinRequest
      sendAdminResponse adminId (MkResponse reqId mLeaderId Admin.JoinInitiated)
    LeaveCluster ->
      use currentLeader
        >>= \case
          Nothing -> sendAdminResponse adminId (MkResponse reqId mLeaderId (Admin.NotLeader Nothing))
          Just leaderId -> do
            self >>= sendRPC leaderId . CM . ClusterMembershipLeaveRequest
            sendAdminResponse adminId (MkResponse reqId mLeaderId Admin.LeaveInitiated)
