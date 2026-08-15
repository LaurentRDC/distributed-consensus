{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedRecordDot #-}

module Network.Consensus.Raft.Transformer
  ( module Network.Consensus.Raft.Transformer.Definition,
    module Network.Consensus.Raft.Transformer.Spec,

    -- * Cluster nodes
    self,
    peers,
    clusterNodes,

    -- * Capabilities
    dequeueEvent,
    enqueueEvent,
    sendRPC,
    sendRPCConcurrently,
    sendRPCResult,
    sendClientResponse,
    whenRole,
    sendHeartbeat,
    sendAppendEntriesTo,
    applyLogEntries,
    nextElectionTimeout,
    updateTerm,
    trace,
    acceptClientRequest,
    applySnapshot,
    currentSnapshot,
    appendLogEntry,

    -- ** Roles
    becomeFollower,
    becomeNonMember,
    becomeLeader,
    becomeCandidate,

    -- ** Elections
    checkElection,

    -- ** Timers
    resetHeartBeatTimer,
    resetElectionTimer,
  )
where

import Control.Concurrent.Class.MonadMVar (MonadMVar)
import Control.Concurrent.Class.MonadSTM (atomically, readTQueue, writeTQueue)
import Control.Monad (unless, when)
import Control.Monad.Class.MonadAsync (MonadAsync, mapConcurrently_)
import Control.Monad.Class.MonadFork (MonadFork)
import Control.Monad.Class.MonadSTM (MonadSTM)
import Control.Monad.Class.MonadThrow (MonadMask)
import Control.Monad.Class.MonadTimer (MonadDelay)
import Control.Monad.Trans.Class (lift)
import Data.Foldable (for_, traverse_)
import qualified Data.Foldable as Foldable
import Data.Function ((&))
import Data.Functor (($>), (<&>))
import qualified Data.Map.Strict as Map
import Data.Maybe (fromJust, isJust)
import Data.Sequence (ViewR (..))
import qualified Data.Sequence as Seq
import Data.Set (Set)
import qualified Data.Set as Set
import qualified Data.Vector as Vector
import Lens.Micro.Platform (assign, at, use, view, (%=), (+=), (.=), (<%=), (^.))
import Network.Consensus.Raft.Client (Response (..))
import Network.Consensus.Raft.Domain (ClusterConfiguration (..), RequestId, Role (..), Term, allNodes, hasQuorum)
import Network.Consensus.Raft.Log (Lookup (..), Snapshot (Snapshot, sData, sMetadata), SnapshotMetadata (..), absoluteIndex, logEntries, sCluster)
import qualified Network.Consensus.Raft.Log as Log
import Network.Consensus.Raft.Timer (Microseconds, resetTimer)
import Network.Consensus.Raft.Transformer.Definition
import Network.Consensus.Raft.Transformer.Spec hiding (sendClientResponse, sendRPC, sendRPCResult)
import qualified Network.Consensus.Raft.Transformer.Spec as Spec
import System.Random (uniformR)

-- | Node identifier
self :: (Monad m) => RaftT entry node state result m node
self = view configuration <&> nodeId

-- | All nodes in the cluster, except self
peers :: (Monad m, Ord node) => RaftT entry node state result m (Set node)
peers = do
  s <- self
  use clusterConfiguration <&> Set.delete s . allNodes

-- | All nodes in the cluster; both 'self' and 'peers'.
clusterNodes :: (Monad m, Ord node) => RaftT entry node state result m (Set node)
clusterNodes = use clusterConfiguration <&> allNodes

dequeueEvent :: (MonadSTM m) => RaftT entry node state result m (Event node entry result state)
dequeueEvent = do
  queue <- view eventQueue
  lift $ atomically $ readTQueue queue

enqueueEvent :: (MonadSTM m) => Event node entry result state -> RaftT entry node state result m ()
enqueueEvent event = do
  queue <- view eventQueue
  lift $ atomically $ writeTQueue queue event

-- | Send a 'RPC' to another node.
--
-- To send a 'RPC' to multiple other nodes, concurrently, see 'sendRPCConcurrently'
sendRPC :: (Monad m) => node -> RPC node entry state -> RaftT entry node state result m ()
sendRPC node rpc = do
  spec <- view specification
  lift $ (spec ^. Spec.sendRPC) node rpc

-- | Send a 'RPC' concurrently to other nodes.
--
-- To send a 'RPC' to a single node, see 'sendRPC'
sendRPCConcurrently :: (MonadAsync m) => Set node -> RPC node entry state -> RaftT entry node state result m ()
sendRPCConcurrently nodes rpc = do
  spec <- view specification
  lift $ mapConcurrently_ (flip (spec ^. Spec.sendRPC) rpc) nodes

sendRPCResult :: (Monad m) => node -> RPCResult node result -> RaftT entry node state result m ()
sendRPCResult node rpc = do
  spec <- view specification
  lift $ (spec ^. Spec.sendRPCResult) node rpc

sendClientResponse :: (Monad m) => node -> Response node result -> RaftT entry node state result m ()
sendClientResponse node response = do
  spec <- view specification
  lift $ (spec ^. Spec.sendClientResponse) node response

whenRole ::
  (Monad m) =>
  Role ->
  RaftT entry node state result m () ->
  RaftT entry node state result m ()
whenRole r action = do
  ourRole <- use role
  when (ourRole == r) action

-- | Send a heartbeat RPC concurrently to all other nodes.
--
-- This function also resets the heartbeat timer.
sendHeartbeat ::
  ( Ord node,
    MonadDelay m,
    MonadMVar m,
    MonadFork m,
    MonadMask m,
    MonadAsync m
  ) =>
  RaftT entry node state result m ()
sendHeartbeat =
  whenRole Leader $ do
    config <- view configuration
    thisTerm <- use term
    lastLogIndex <- use lastApplied
    theCommitIndex <- use commitIndex
    let rpc = HeartBeat thisTerm config.nodeId lastLogIndex theCommitIndex
    peers >>= (`sendRPCConcurrently` rpc)
    view heartBeatTimer >>= lift . resetTimer config.heartBeatTimeout

-- | For a given destination node, look up which of the entries
-- in our log should be replicated, and send an appropriate 'AppendEntries' RPC.
sendAppendEntriesTo :: (Ord node, Monad m) => node -> RaftT entry node state result m ()
sendAppendEntriesTo destination = do
  entries <- use commandLog
  use (nextIndex . at destination)
    >>= ( \case
            Nothing -> pure $ Just (0, initialTerm)
            Just destNextIndex ->
              let previousLogIndex = pred destNextIndex
               in case entries Log.!? previousLogIndex of
                    Found (prevTerm, _) -> pure $ Just (previousLogIndex, prevTerm)
                    NotFound -> pure $ Just (0, initialTerm)
                    -- The follower's next index is "lost" in the snapshot;
                    -- we can't specify entries to append. A snapshot will have
                    -- to be installed
                    LogIndexInSnapshot (SnapshotMetadata lastSnapshotIndex lastSnapshotTerm)
                      | previousLogIndex == lastSnapshotIndex -> pure $ Just (lastSnapshotIndex, lastSnapshotTerm)
                      | otherwise -> pure Nothing
        )
    >>= \case
      Just (previousLogIndex, previousLogTerm) -> do
        let toBeReplicated = Seq.drop (fromIntegral $ Log.relativeIndex entries previousLogIndex) (logEntries entries)
        unless
          (null toBeReplicated)
          ( ( AppendEntries
                <$> use term
                <*> (view configuration <&> nodeId)
                <*> pure previousLogIndex
                <*> pure previousLogTerm
                <*> pure (Vector.fromList (Foldable.toList toBeReplicated))
                <*> use commitIndex
            )
              >>= sendRPC destination . AE
          )
      Nothing ->
        ( InstallSnapshot
            <$> use term
            <*> (view configuration <&> nodeId)
            <*> currentSnapshot
        )
          >>= sendRPC destination . IS

applyEntry :: (MonadSTM m, Ord node, MonadMVar m) => LogEntry node entry -> RaftT entry node state result m (Maybe (CommandResponse node result))
applyEntry (LogEntryCommand (Command entry requestId)) = do
  apply <- view (specification . applyLogEntry)
  (newState, result) <- use internalState <&> flip apply entry
  trace (\n t -> LogEntryApplied n t entry)

  internalState .= newState
  pure $ Just (MkCommandResponse result requestId)
applyEntry (LogEntryMembershipChange clusterConf) = do
  assign clusterConfiguration clusterConf
  trace (\n t -> MembershipChangeApplied n t clusterConf)

  -- If we just committed to a joint membership configuration,
  -- then it's time to propose the move to the union
  case clusterConf of
    Simple _ -> trace MembershipChangeCompleted $> Nothing
    Joint _before after -> do
      appendLogEntry (LogEntryMembershipChange (Simple after))
      pure Nothing

-- | Updates the commit index. Returns 'True' if some log entries can be applied,
-- and 'False' otherwise.
updateCommitIndex :: (Ord node, Monad m) => RaftT entry node state result m Bool
updateCommitIndex = do
  commitIndex' <- use commitIndex
  entries <- use commandLog
  ourTerm <- use term
  matchIndices <- use matchIndex
  s <- self
  clusterConf <- use clusterConfiguration

  -- Fetch the indices, starting from the log index after the commit index, such that:
  -- 1. the entry is associated with the current term
  -- 2. a quorum of nodes have committed this index
  --
  -- All of this is done relative to the snapshot, hence why all of the
  -- indices are relativized
  let relativeMatchIndices = fmap (Log.relativeIndex entries) matchIndices
      relativeCommitIndex = Log.relativeIndex entries commitIndex'
      liveEntries = logEntries entries
      relativeQuorumIndices =
        Seq.zip
          ( Seq.fromFunction
              (length liveEntries)
              -- The initial commit index is 0, hence the first entry to be committed
              -- should have index 1
              succ
          )
          liveEntries
          & Seq.drop (fromIntegral relativeCommitIndex)
          & Seq.filter ((== ourTerm) . fst . snd)
          & fmap fst
          -- By definition of the Raft algorithm, only a contiguous subset of
          -- indices can have been accepted with a quorum, hence the use of
          -- takeWhileL
          & Seq.takeWhileL
            ( \i ->
                hasQuorum
                  clusterConf
                  -- Note that 'matchIndices' do not include the leader,
                  -- so we take the leader into account by inserting it
                  -- with the other match indices
                  ( Set.insert s $
                      Map.keysSet
                        (Map.filter (>= fromIntegral i) relativeMatchIndices)
                  )
            )
          & fmap fromIntegral

  case Seq.viewr relativeQuorumIndices of
    Seq.EmptyR -> pure False
    _ :> relativeLastIndex -> do
      let lastIndex = absoluteIndex entries relativeLastIndex
      commitIndex .= lastIndex
      trace (\n t -> CommitIndexIncreasedTo n t lastIndex)
      pure True

-- | Update the commit index, and apply log entries if there are
-- any log entries that /can/ be applied.
applyLogEntries :: (Ord node, MonadMVar m, MonadSTM m) => RaftT entry node state result m ()
applyLogEntries = do
  isTimeToCommit <- updateCommitIndex
  when isTimeToCommit $ do
    lastAppliedIndex <- use lastApplied
    currentCommitIndex <- use commitIndex
    unless (lastAppliedIndex == currentCommitIndex) $ do
      entries <- use commandLog
      let unAppliedEntries =
            entries
              & (`Log.keepEntriesUpTo` currentCommitIndex)
              & logEntries
              & Seq.drop (fromIntegral (Log.relativeIndex entries lastAppliedIndex))
              & fmap snd

      results <-
        mapM applyEntry unAppliedEntries
          -- Membership change commands don't return a result
          <&> fmap fromJust . Seq.filter isJust

      whenRole Leader $ do
        leaderId <- self
        -- Initially I tried to use `withMVar requestsVar (... putMVar <some leaf MVar>)`,
        -- but this caused a rare race condition uncovered by `io-sim`!
        for_ results $ \response@(MkCommandResponse result requestId) ->
          use currentClientRequests
            <&> Map.lookup requestId
            >>= \case
              Nothing -> pure () -- TODO: isn't this unexpected?
              Just requester -> do
                trace (\t n -> CommandResultResponded t n response)
                -- TODO: reply (and possibly retry) in a separate thread
                sendClientResponse requester (Success leaderId result)
                currentClientRequests %= Map.delete requestId

      lastApplied .= currentCommitIndex
      trace (\n t -> LastAppliedIndexIncreasedTo n t currentCommitIndex)

      view configuration <&> maxLogLength >>= \case
        Nothing -> pure ()
        Just maxLog -> do
          entries' <- use commandLog
          when
            (Seq.length (logEntries entries') > maxLog)
            (currentSnapshot >>= applySnapshot)

nextElectionTimeout :: (Monad m) => RaftT entry node state result m Microseconds
nextElectionTimeout = do
  gen <- use randomGen
  (lowerBound, upperBound) <- view configuration <&> electionTimeoutRange
  let (timeout, nextGen) = uniformR (lowerBound, upperBound) gen
  randomGen .= nextGen
  pure timeout

trace :: (Monad m) => (Term -> node -> RaftTrace entry result node state) -> RaftT entry node state result m ()
trace makeTrace = do
  ourTerm <- use term
  node <- view configuration <&> nodeId
  spec <- view specification
  lift $ (spec ^. tracer) (makeTrace ourTerm node)

-- | Update the internal term state, returning the previous and new 'Term's.
--
-- If the update function returns the current term, this function does nothing.
updateTerm :: (Monad m) => (Term -> Term) -> RaftT entry node state result m (Term, Term)
updateTerm update = do
  currTerm <- use term
  s <- self
  let newTerm = update currTerm

  when (newTerm > currTerm) $ do
    spec <- view specification

    lift $ (spec ^. writeTerm) s newTerm
    term .= newTerm
  pure (currTerm, newTerm)

-- | Set up a callback for a client request
acceptClientRequest ::
  (MonadMVar m) =>
  -- | Client node
  node ->
  RaftT entry node state result m RequestId
acceptClientRequest clientId = do
  requestId <- nextRequestId <%= succ
  currentClientRequests %= Map.insert requestId clientId

  pure requestId

currentSnapshot :: (Monad m) => RaftT entry node state result m (Snapshot node state)
currentSnapshot =
  Snapshot
    <$> ( SnapshotMetadata
            <$> use lastApplied
            <*> use term
        )
    <*> use internalState
    <*> use clusterConfiguration

-- | Apply a snapshot to the internal state.
--
-- This can be used by any node on itself, or
-- as part of the 'InstallSnapshot' remote procedure call.
applySnapshot :: (Monad m) => Snapshot node state -> RaftT entry node state result m ()
applySnapshot snapshot = do
  s <- self
  spec <- view specification
  -- TODO: initiate the snapshot write in a separate
  -- thread, with a finalizer to apply the snapshot to the log
  lift $ (spec ^. writeSnapshot) s snapshot
  commandLog %= Log.applySnapshot snapshot
  internalState .= sData snapshot
  clusterConfiguration .= sCluster snapshot
  trace (\t n -> SnapshotApplied t n (sMetadata snapshot))

-- | Append a log entry to the log
appendLogEntry :: (Ord node, MonadMVar m, MonadSTM m) => LogEntry node entry -> RaftT entry node state result m ()
appendLogEntry entry = do
  ourTerm <- use term
  commandLog %= (`Log.append` (ourTerm, entry))
  trace (\t n -> LogEntryAppended t n entry)

  -- TODO: sendAppendEntriesTo in parallel
  whenRole Leader $
    peers >>= traverse_ sendAppendEntriesTo

  -- In a single-node cluster, we would already have
  -- quorum to update our commit index
  applyLogEntries

becomeLeader ::
  ( Ord node,
    MonadDelay m,
    MonadMVar m,
    MonadFork m,
    MonadMask m,
    MonadAsync m
  ) =>
  RaftT entry node state result m ()
becomeLeader = do
  role .= Leader
  self <&> Just >>= assign currentLeader
  yesVotes .= Set.empty

  lastIx <- use commandLog <&> Log.lastLogIndex
  peers <&> Map.fromSet (const (lastIx + 1)) >>= assign nextIndex

  trace LeaderElected

  -- TODO: send append all entries messages to all followers
  sendHeartbeat -- Note that 'sendHeartbeat' will also reset the heartbeat timer

becomeFollower ::
  (MonadMVar m, MonadDelay m, MonadFork m, MonadMask m, MonadAsync m) =>
  -- | Leader node ID
  node ->
  RaftT entry node state result m ()
becomeFollower leaderNodeId = do
  resetHeartBeatTimer
  resetElectionTimer
  r <- use role
  unless (r == Follower) $ do
    trace BecameFollower
    role .= Follower
  currentLeader .= Just leaderNodeId

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

  s <- self
  term += 1
  w <- view (specification . writeTerm)
  thisTerm <- use term
  lift (w s thisTerm)

  v <- view (specification . voteFor)
  lift $ v s (Just s)
  trace (VotedFor thisTerm s)
  votedFor .= Just s
  yesVotes .= Set.singleton s

  resetElectionTimer

  checkElection -- there might only be a single node in the cluster
  r <- use role
  when (r == Candidate) $ do
    currentTerm <- use term
    lastAppliedLogIndex <- use lastApplied
    lastLogTerm <-
      use commandLog <&> snd . Log.lastLogInfo
    let rpc = RequestVote currentTerm s lastAppliedLogIndex lastLogTerm
    peers >>= (`sendRPCConcurrently` rpc)

checkElection ::
  ( Ord node,
    MonadAsync m,
    MonadMask m,
    MonadFork m,
    MonadMVar m,
    MonadDelay m
  ) =>
  RaftT entry node state result m ()
checkElection = do
  q <- hasQuorum <$> use clusterConfiguration <*> use yesVotes
  when q becomeLeader

becomeNonMember ::
  (MonadMVar m) =>
  RaftT entry node state result m ()
becomeNonMember = do
  r <- use role
  unless (r == NonMember) $ do
    trace BecameNonMember
    role .= NonMember
  currentLeader .= Nothing

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
