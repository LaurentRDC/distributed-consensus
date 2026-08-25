{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE TupleSections #-}

module Distributed.Consensus.Raft.Transformer
  ( module Distributed.Consensus.Raft.Transformer.Definition,

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
    sendAdminResponse,
    whenRole,
    sendHeartbeat,
    sendAppendEntriesTo,
    voteFor,
    applyLogEntries,
    applyCommittedEntries,
    nextElectionTimeout,
    updateTerm,
    trace,
    acceptClientRequests,
    persistSnapshot,
    applySnapshot,
    adoptConfigurationFromLog,
    isClusterMember,
    currentSnapshot,
    appendLogEntries,
    restoreState,

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

import Control.Concurrent.Class.MonadMVar (MonadMVar, takeMVar)
import Control.Concurrent.Class.MonadSTM (atomically, readTQueue, writeTQueue)
import Control.Monad (unless, void, when)
import Control.Monad.Class.MonadAsync (MonadAsync, async, mapConcurrently_, race)
import Control.Monad.Class.MonadFork (MonadFork)
import Control.Monad.Class.MonadSTM (MonadSTM)
import Control.Monad.Class.MonadThrow (MonadMask)
import Control.Monad.Class.MonadTimer (MonadDelay)
import Control.Monad.Trans.Class (lift)
import Data.Foldable (for_, traverse_)
import qualified Data.Foldable as Foldable
import Data.Function ((&))
import Data.Functor (($>), (<&>))
import Data.List.NonEmpty (NonEmpty)
import qualified Data.List.NonEmpty as NonEmpty
import qualified Data.Map.Strict as Map
import Data.Maybe (fromJust, isJust)
import Data.Sequence (ViewR (..), (|>))
import qualified Data.Sequence as Seq
import Data.Set (Set)
import qualified Data.Set as Set
import Data.Traversable (for)
import Distributed.Consensus.Raft.Admin (AdminResponse)
import Distributed.Consensus.Raft.Client (ClientRequest, ClientResponse, ClientResult (..))
import Distributed.Consensus.Raft.Domain (ClusterConfiguration (..), RequestId (clientRequestId), Role (..), Snapshot (..), SnapshotMetadata (..), Term, allNodes, hasQuorum, mkRequestId)
import Distributed.Consensus.Raft.Implementation hiding (sendAdminResponse, sendClientResponse, sendRPC, sendRPCResult)
import qualified Distributed.Consensus.Raft.Implementation as Impl
import Distributed.Consensus.Raft.Log (Lookup (..), absoluteIndex, logEntries)
import qualified Distributed.Consensus.Raft.Log as Log
import Distributed.Consensus.Raft.Messaging (Request (..), Response (..))
import Distributed.Consensus.Raft.Timer (Microseconds, resetTimer)
import Distributed.Consensus.Raft.Transformer.Definition
import Lens.Micro.Platform (assign, at, use, view, (%=), (+=), (.=), (<%=))
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

-- | Returns @Nothing@ if the exit lock has been unlocked, in which case
-- the server should shut down
dequeueEvent :: (MonadMVar m, MonadAsync m) => RaftT entry node state result m (Maybe (Event node entry result state))
dequeueEvent = do
  queue <- view eventQueue
  lock <- view exitLock
  lift (race (takeMVar lock) (atomically (readTQueue queue)))
    >>= either (const (pure Nothing)) (pure . Just)

enqueueEvent :: (MonadSTM m) => Event node entry result state -> RaftT entry node state result m ()
enqueueEvent event = do
  queue <- view eventQueue
  lift $ atomically $ writeTQueue queue event

-- | Send a 'RPC' to another node.
--
-- To send a 'RPC' to multiple other nodes, concurrently, see 'sendRPCConcurrently'
sendRPC :: (Monad m) => node -> RPC node entry state -> RaftT entry node state result m ()
sendRPC node rpc = do
  impl <- view implementation
  lift $ impl.networking.sendRPC node rpc

-- | Send a 'RPC' concurrently to other nodes.
--
-- To send a 'RPC' to a single node, see 'sendRPC'
sendRPCConcurrently :: (MonadAsync m) => Set node -> RPC node entry state -> RaftT entry node state result m ()
sendRPCConcurrently nodes rpc = do
  impl <- view implementation
  lift $ mapConcurrently_ (flip impl.networking.sendRPC rpc) nodes

sendRPCResult :: (Monad m) => node -> RPCResult node result -> RaftT entry node state result m ()
sendRPCResult node rpc = do
  impl <- view implementation
  lift $ impl.networking.sendRPCResult node rpc

sendClientResponse :: (Monad m) => node -> ClientResponse node result -> RaftT entry node state result m ()
sendClientResponse node response = do
  impl <- view implementation
  lift $ impl.networking.sendClientResponse node response

sendAdminResponse :: (Monad m) => node -> AdminResponse node -> RaftT entry node state result m ()
sendAdminResponse admin response = do
  impl <- view implementation
  lift $ impl.networking.sendAdminResponse admin response

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
    -- TODO: send append entries in parallel
    peers >>= traverse_ sendAppendEntriesTo
    view heartBeatTimer >>= lift . resetTimer config.heartBeatTimeout

-- | For a given destination node, look up which of the entries
-- in our log should be replicated, and send an appropriate 'AppendEntries' RPC.
--
-- Even if a node
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
        -- We send 'AppendEntries' event if 'toBeReplicated'
        -- is empty, as this serves as a heartbeat mechanism
        ( AppendEntries
            <$> use term
            <*> (view configuration <&> nodeId)
            <*> pure previousLogIndex
            <*> pure previousLogTerm
            <*> pure toBeReplicated
            <*> use commitIndex
          )
          >>= sendRPC destination . AE
      Nothing ->
        ( InstallSnapshot
            <$> use term
            <*> (view configuration <&> nodeId)
            <*> currentSnapshot
        )
          >>= sendRPC destination . IS

applyEntry ::
  ( Ord node,
    MonadMVar m,
    MonadAsync m,
    MonadMask m,
    MonadFork m,
    MonadDelay m
  ) =>
  LogEntry node entry -> RaftT entry node state result m (Maybe (CommandResponse node result))
applyEntry (LogEntryCommand (Command reqId entry)) = do
  apply <- view implementation <&> applyLogEntry
  (newState, result) <- use internalState <&> flip apply entry
  trace (`LogEntryApplied` entry)

  internalState .= newState
  pure $ Just (MkCommandResponse result reqId)
applyEntry (LogEntryMembershipChange clusterConf) = do
  -- Cluster membership happens at append-time, not at apply-time
  -- See 'adoptConfigurationFromLog'

  -- If we just committed to a joint membership configuration,
  -- then it's time to propose the move to the union
  case clusterConf of
    Simple cluster -> do
      s <- self
      whenRole Leader $ unless (s `Set.member` cluster) (becomeFollower Nothing)
      trace MembershipChangeCompleted $> Nothing
    Joint _before after -> do
      whenRole Leader $
        appendLogEntries (NonEmpty.singleton (LogEntryMembershipChange (Simple after)))
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
      trace (`CommitIndexIncreasedTo` lastIndex)
      pure True

-- | Set the current vote, both in the internal state
-- but also in persisted state.
--
-- This function doesn't emit any RPCs
voteFor :: (Monad m) => Maybe node -> RaftT entry node state result m ()
voteFor mNode = do
  impl <- view implementation
  s <- self
  t <- use term
  lift $ writeVotedFor impl s t mNode
  votedFor .= mNode
  traverse_ (\n -> trace (\ctx -> VotedFor ctx t n)) mNode

-- | Update the commit index, and apply log entries if there are
-- any log entries that /can/ be applied.
applyLogEntries :: (Ord node, MonadMVar m, MonadAsync m, MonadMask m, MonadFork m, MonadDelay m) => RaftT entry node state result m ()
applyLogEntries = do
  isTimeToCommit <- updateCommitIndex
  when isTimeToCommit applyCommittedEntries

applyCommittedEntries :: (Ord node, MonadMVar m, MonadAsync m, MonadMask m, MonadFork m, MonadDelay m) => RaftT entry node state result m ()
applyCommittedEntries = do
  lastAppliedIndex <- use lastApplied
  currentCommitIndex <- use commitIndex
  when (lastAppliedIndex < currentCommitIndex) $ do
    entries <- use commandLog
    let unAppliedEntries =
          entries
            & (`Log.keepEntriesUpTo` currentCommitIndex)
            & logEntries
            & Seq.drop (fromIntegral (Log.relativeIndex entries lastAppliedIndex))
            & fmap snd

    -- Claim the range before applying any of it. 'applyEntry' can re-enter this
    -- function -- a committed joint configuration makes the leader append the
    -- continuation, and appending applies -- and if 'lastApplied' were still
    -- pointing at the start of the range, that nested call would apply the same
    -- entries over again, append again, and never stop.
    lastApplied .= currentCommitIndex
    trace (`LastAppliedIndexIncreasedTo` currentCommitIndex)

    results <-
      mapM applyEntry unAppliedEntries
        -- Membership change commands don't return a result
        <&> fmap fromJust . Seq.filter isJust

    whenRole Leader $ do
      leaderId <- self

      let resultsByReqId =
            results
              & Foldable.toList
              & map (\(MkCommandResponse result reqId) -> (reqId, NonEmpty.singleton result))
              & Map.fromListWith (<>)
              & Map.map NonEmpty.reverse -- Necessary because 'fromListWith' reverses order of applied commands
      for_ (Map.toList resultsByReqId) $ \(reqId, responses) ->
        use currentClientRequests
          <&> Map.lookup reqId
          >>= \case
            Nothing -> pure () -- TODO: isn't this unexpected?
            Just requester -> do
              for_ responses $ \response -> do
                -- TODO: reply (and possibly retry) in a separate thread
                sendClientResponse requester (MkResponse (clientRequestId reqId) (Just leaderId) (Success response))
                trace (`CommandResultResponded` MkCommandResponse response reqId)
              currentClientRequests %= Map.delete reqId

    view configuration <&> maxLogLength >>= \case
      Nothing -> pure ()
      Just maxLog -> do
        entries' <- use commandLog
        when
          (Seq.length (logEntries entries') > maxLog)
          (currentSnapshot >>= persistSnapshot)

nextElectionTimeout :: (Monad m) => RaftT entry node state result m Microseconds
nextElectionTimeout = do
  gen <- use randomGen
  (lowerBound, upperBound) <- view configuration <&> electionTimeoutRange
  let (timeout, nextGen) = uniformR (lowerBound, upperBound) gen
  randomGen .= nextGen
  pure timeout

trace :: (Monad m) => (EventContext node -> RaftTrace entry result node state) -> RaftT entry node state result m ()
trace makeTrace = do
  eventCtx <- EventContext <$> use term <*> self
  impl <- view implementation
  lift $ tracer impl (makeTrace eventCtx)

-- | Update the internal term state, returning the previous and new 'Term's.
--
-- If the update function returns the current term, this function does nothing.
updateTerm :: (Monad m) => (Term -> Term) -> RaftT entry node state result m (Term, Term)
updateTerm update = do
  currTerm <- use term
  s <- self
  let newTerm = update currTerm

  when (newTerm > currTerm) $ do
    impl <- view implementation

    lift $ writeTerm impl s newTerm
    term .= newTerm
  pure (currTerm, newTerm)

-- | Set up a callback for a client request
acceptClientRequests ::
  (MonadMVar m) =>
  -- | Client node
  NonEmpty (ClientRequest node entry) ->
  RaftT entry node state result m (NonEmpty RequestId)
acceptClientRequests requests = do
  -- The term is part of the request ID. Our internal counter is volatile and
  -- restarts from zero whenever we boot, so without the term an entry stamped
  -- by an earlier leader could collide with one of ours, and we would answer
  -- the client with the result of that older command. See 'RequestId'.
  ourTerm <- use term
  for requests $ \(MkRequest clientReqId clientId _) -> do
    internalReqId <- nextRequestId <%= succ
    let reqId = mkRequestId ourTerm internalReqId clientReqId
    currentClientRequests %= Map.insert reqId clientId
    pure reqId

currentSnapshot :: (Monad m) => RaftT entry node state result m (Snapshot node state)
currentSnapshot = do
  lastAppliedIndex <- use lastApplied
  entries <- use commandLog
  ourTerm <- use term

  -- The snapshot stands in for the log up to and including 'lastAppliedIndex',
  -- so its term has to be the term of the entry /at/ that index, not the term we
  -- happen to be in now. Those differ whenever we snapshot in a later term than
  -- the entries we are compacting, and getting it wrong wedges a follower for
  -- good: it installs the snapshot, the leader then sends 'AppendEntries' with
  -- the real term for that index, the consistency check against the snapshot
  -- metadata fails, the leader backs up into the snapshot and installs it
  -- again, forever.
  let lastAppliedTerm = case entries Log.!? lastAppliedIndex of
        Found (entryTerm, _) -> entryTerm
        -- Already compacted into our own snapshot, whose metadata knows the term.
        LogIndexInSnapshot (SnapshotMetadata _ snapshotTerm) -> snapshotTerm
        -- Cannot happen: 'lastApplied' never runs past the log.
        NotFound -> ourTerm

  Snapshot (SnapshotMetadata lastAppliedIndex lastAppliedTerm)
    <$> use internalState
    <*> use clusterConfiguration

-- | Serialize the snapshot in a separate thread.
--
-- An event will be emitted to run `applySnapshot`
persistSnapshot :: (MonadAsync m) => Snapshot node state -> RaftT entry node state result m ()
persistSnapshot snapshot = do
  s <- self
  impl <- view implementation
  queue <- view eventQueue

  void $ lift $ async $ do
    writeSnapshot impl s snapshot
    atomically $
      writeTQueue
        queue
        (EventSnapshotPersisted snapshot)

-- | Take the cluster configuration from the log, and bring our role into line
-- with it.
adoptConfigurationFromLog :: (Monad m) => RaftT entry node state result m ()
adoptConfigurationFromLog = do
  entries <- use commandLog <&> logEntries

  let latestLoggedConfiguration =
        Foldable.foldl'
          ( \acc -> \case
              (_, LogEntryMembershipChange conf) -> Just conf
              _ -> acc
          )
          Nothing
          entries
  -- With no membership entry in the log, whatever the snapshot gave us stands.
  traverse_ (assign clusterConfiguration) latestLoggedConfiguration
  traverse_ (\conf -> trace (`MembershipChangeApplied` conf)) latestLoggedConfiguration

-- | Whether we are in the configuration we are currently using.
isClusterMember :: (Ord node, Monad m) => RaftT entry node state result m Bool
isClusterMember = do
  s <- self
  use clusterConfiguration <&> Set.member s . allNodes

-- | Apply a snapshot to the internal state.
--
-- This can be used by any node on itself, or
-- as part of the 'InstallSnapshot' remote procedure call.
applySnapshot :: (Monad m) => Snapshot node state -> RaftT entry node state result m ()
applySnapshot snapshot = do
  commandLog %= Log.applySnapshot snapshot.sMetadata
  internalState .= sData snapshot
  clusterConfiguration .= sCluster snapshot

  -- In case we're applying a snapshot on state restore,
  -- the commit index and last applied index might be
  -- wrong
  commitIndex %= max snapshot.sMetadata.smLastIncludedIndex
  lastApplied %= max snapshot.sMetadata.smLastIncludedIndex

  trace (`SnapshotApplied` sMetadata snapshot)

-- | Apply one or more entries to the log, from the leader's point of view
appendLogEntries ::
  ( Ord node,
    MonadMVar m,
    MonadAsync m,
    MonadMask m,
    MonadFork m,
    MonadDelay m
  ) =>
  NonEmpty (LogEntry node entry) ->
  RaftT entry node state result m ()
appendLogEntries entries = do
  ourTerm <- use term
  s <- self
  impl <- view implementation
  startingLogIndex <- use commandLog <&> Log.lastLogIndex
  commandLog %= (`Log.extend` ((ourTerm,) <$> Seq.fromList (NonEmpty.toList entries)))

  let batchToWrite = zip3 [succ startingLogIndex ..] (repeat ourTerm) (NonEmpty.toList entries)
  lift $ Impl.writeLogEntry impl s batchToWrite
  for_ batchToWrite $ \(ix, _, entry) ->
    trace (\ctx -> LogEntryAppended ctx ix entry)

  adoptConfigurationFromLog

  -- It's important to emit this event because a new leader may have its
  -- log index jump. Without having visibility into this jump,
  -- it's hard to check that the commit index is always <= the last log index
  use commandLog <&> Log.lastLogIndex >>= trace . flip LastLogIndexChangedTo

  -- TODO: sendAppendEntriesTo in parallel
  whenRole Leader $
    peers >>= traverse_ sendAppendEntriesTo

  -- In a single-node cluster, we would already have
  -- quorum to update our commit index
  applyLogEntries

restoreState :: (Monad m) => RaftT entry node state result m ()
restoreState = do
  impl <- view implementation
  s <- self

  (persistedTerm, persistedVote, mPersistedSnapshot) <-
    lift $ do
      t <- readTerm impl s
      (t,,)
        <$> readVotedFor impl s t
        <*> readSnapshot impl s

  term .= persistedTerm
  votedFor .= persistedVote

  let snapshotIndex = maybe 0 (smLastIncludedIndex . sMetadata) mPersistedSnapshot

  entries <-
    lift $
      let loop ix acc =
            readLogEntry impl s ix >>= \case
              Nothing -> pure acc
              Just entry -> loop (succ ix) (acc |> entry)
       in loop (succ snapshotIndex) Seq.empty

  let restoredLog = Log.buildLog entries (sMetadata <$> mPersistedSnapshot)
  commandLog .= restoredLog
  traverse_ applySnapshot mPersistedSnapshot

  -- The snapshot only carries the configuration in the
  -- snapshot, but entries might have changed it
  adoptConfigurationFromLog

  trace
    ( \ctx ->
        StateRestored
          ctx
          persistedVote
          restoredLog
    )

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

  -- cluster configuration could be ongoing, started under a different leader
  use clusterConfiguration >>= \case
    Simple _ -> pure ()
    Joint _before after ->
      appendLogEntries (NonEmpty.singleton (LogEntryMembershipChange (Simple after)))

  -- TODO: send append all entries messages to all followers
  sendHeartbeat -- Note that 'sendHeartbeat' will also reset the heartbeat timer

becomeFollower ::
  (MonadMVar m, MonadDelay m, MonadFork m, MonadMask m, MonadAsync m) =>
  -- | Leader if known
  Maybe node ->
  RaftT entry node state result m ()
becomeFollower mLeaderNodeId = do
  resetHeartBeatTimer
  resetElectionTimer
  r <- use role
  unless (r == Follower) $ do
    trace BecameFollower
    role .= Follower
  currentClientRequests .= Map.empty
  traverse_ (\leaderNodeId -> currentLeader .= Just leaderNodeId) mLeaderNodeId

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
  w <- view implementation <&> writeTerm
  thisTerm <- use term
  lift (w s thisTerm)

  voteFor (Just s)
  yesVotes .= Set.singleton s

  resetElectionTimer

  checkElection -- there might only be a single node in the cluster
  r <- use role
  when (r == Candidate) $ do
    currentTerm <- use term
    (lastLogIx, lastLogTerm) <- use commandLog <&> Log.lastLogInfo
    let rpc = RequestVote currentTerm s lastLogIx lastLogTerm
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
