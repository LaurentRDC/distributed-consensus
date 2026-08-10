{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedRecordDot #-}

module Network.Consensus.Raft.Transformer
  ( module Network.Consensus.Raft.Transformer.Definition,
    module Network.Consensus.Raft.Transformer.Spec,

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
    quorum,
    applyLogEntries,
    nextElectionTimeout,
    updateTerm,
    trace,
    acceptClientRequest,
    applySnapshot,
  )
where

import Control.Concurrent.Class.MonadMVar (MonadMVar, modifyMVar_, newEmptyMVar, putMVar, readMVar, withMVar)
import Control.Concurrent.Class.MonadSTM (atomically, readTQueue, writeTQueue)
import Control.Monad (unless, void, when)
import Control.Monad.Class.MonadAsync (MonadAsync, mapConcurrently_)
import Control.Monad.Class.MonadFork (MonadFork, forkFinally, labelThisThread)
import Control.Monad.Class.MonadSTM (MonadSTM)
import Control.Monad.Class.MonadThrow (MonadMask)
import Control.Monad.Class.MonadTimer (MonadDelay)
import Control.Monad.Trans.Class (lift)
import Data.Foldable (for_)
import qualified Data.Foldable as Foldable
import Data.Function ((&))
import Data.Functor (($>), (<&>))
import qualified Data.IntMap.Strict as IntMap
import qualified Data.Map.Strict as Map
import Data.Sequence (ViewR (..))
import qualified Data.Sequence as Seq
import Data.Set (Set)
import qualified Data.Set as Set
import Data.Traversable (for)
import qualified Data.Vector as Vector
import Lens.Micro.Platform (at, use, view, (%=), (.=), (<%=), (^.))
import Network.Consensus.Raft.Client (Response (..))
import Network.Consensus.Raft.Domain (RequestId, Role (..), Term)
import Network.Consensus.Raft.Log (Lookup (..), Snapshot (Snapshot, sData, sMetadata), SnapshotMetadata (..), absoluteIndex, logEntries)
import qualified Network.Consensus.Raft.Log as Log
import Network.Consensus.Raft.Timer (Microseconds, resetTimer)
import Network.Consensus.Raft.Transformer.Definition
import Network.Consensus.Raft.Transformer.Spec hiding (sendClientResponse, sendRPC, sendRPCResult)
import qualified Network.Consensus.Raft.Transformer.Spec as Spec
import System.Random (uniformR)

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
  ( MonadDelay m,
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
    let peers = config.otherNodes
    lastLogIndex <- use lastApplied
    theCommitIndex <- use commitIndex
    let rpc = HeartBeat thisTerm config.nodeId lastLogIndex theCommitIndex
    sendRPCConcurrently peers rpc
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

-- | Number of votes required for a decision to be majority.
quorum :: (Monad m) => RaftT entry node state result m Int
quorum = do
  conf <- view configuration
  let numNodesInCluster = 1 + Set.size (otherNodes conf)
  pure $
    if even numNodesInCluster
      then numNodesInCluster `div` 2 + 1
      else (numNodesInCluster - 1) `div` 2 + 1

applyCommand :: (Monad m) => Command entry -> RaftT entry node state result m (CommandResponse node result)
applyCommand (MkCommand entry requestId) = do
  apply <- view (specification . applyLogEntry)
  (newState, result) <- use internalState <&> flip apply entry
  trace (\n t -> LogEntryApplied n t entry)

  internalState .= newState
  pure (MkCommandResponse result requestId)

-- | Updates the commit index. Returns 'True' if some log entries can be applied,
-- and 'False' otherwise.
updateCommitIndex :: (Monad m) => RaftT entry node state result m Bool
updateCommitIndex = do
  commitIndex' <- use commitIndex
  entries <- use commandLog
  ourTerm <- use term
  matchIndices <- use matchIndex
  quorumSize <- quorum

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
                Map.size (Map.filter (>= fromIntegral i) relativeMatchIndices)
                  -- Note that 'matchIndices' do not include the leader,
                  -- so we take the leader into account by ensuring that there are
                  -- at least 'pred quorumSize' other nodes that have accepted the
                  -- entries, rather than 'quorumsize'
                  >= pred quorumSize
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
applyLogEntries :: (MonadMVar m) => RaftT entry node state result m ()
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

      results <- mapM applyCommand unAppliedEntries

      requestsVar <- view currentClientRequests
      whenRole Leader $ do
        responsesQueued <- lift $
          withMVar requestsVar $ \requests ->
            for results $
              \(MkCommandResponse result requestId) ->
                traverse
                  (\r -> r `putMVar` result $> MkCommandResponse result requestId)
                  (IntMap.lookup (fromIntegral requestId) requests)

        for_ responsesQueued $ \case
          Nothing -> pure ()
          Just r -> trace (\t n -> CommandResultResponded t n r)

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
  self <- view configuration <&> nodeId
  let newTerm = update currTerm

  when (newTerm > currTerm) $ do
    spec <- view specification

    lift $ (spec ^. writeTerm) self newTerm
    term .= newTerm
  pure (currTerm, newTerm)

-- | Set up a callback for a client request
acceptClientRequest ::
  (MonadMVar m, MonadFork m) =>
  -- | Client node
  node ->
  RaftT entry node state result m RequestId
acceptClientRequest clientId = do
  requests <- view currentClientRequests
  spec <- view specification
  requestId <- nextRequestId <%= succ
  resultVar <- lift newEmptyMVar
  leaderId <- view configuration <&> nodeId
  lift $
    modifyMVar_ requests $
      pure . IntMap.insert (fromIntegral requestId) resultVar

  void
    $ lift
    $ forkFinally
      ( do
          -- TODO: define some timeout after which this thread is filled,
          -- and the cleanup deletes the data associated with the request ID
          labelThisThread $ "Client request handler for request ID " <> show requestId
          result <- readMVar resultVar
          (spec ^. Spec.sendClientResponse) clientId (Success leaderId result)
      )
    $ \_ ->
      modifyMVar_ requests $
        pure . IntMap.delete (fromIntegral requestId)

  pure requestId

currentSnapshot :: (Monad m) => RaftT entry node state result m (Snapshot state)
currentSnapshot =
  Snapshot
    <$> ( SnapshotMetadata
            <$> use lastApplied
            <*> use term
        )
    <*> use internalState

-- | Apply a snapshot to the internal state.
--
-- This can be used by any node on itself, or
-- as part of the 'InstallSnapshot' remote procedure call.
applySnapshot :: (Monad m) => Snapshot state -> RaftT entry node state result m ()
applySnapshot snapshot = do
  self <- view configuration <&> nodeId
  spec <- view specification
  -- TODO: initiate the snapshot write in a separate
  -- thread, with a finalizer to apply the snapshot to the log
  lift $ (spec ^. writeSnapshot) self snapshot
  commandLog %= Log.applySnapshot snapshot
  internalState .= sData snapshot
  trace (\t n -> SnapshotApplied t n (sMetadata snapshot))
