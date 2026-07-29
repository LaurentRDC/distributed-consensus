{-# LANGUAGE BangPatterns #-}
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
    sendHeartbeat,
    sendAppendEntriesTo,
    quorum,
    applyLogEntries,
    nextElectionTimeout,
    updateTerm,
    trace,
    acceptClientRequest,
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
import Lens.Micro.Platform (at, use, view, (.=), (<%=), (^.))
import Network.Consensus.Raft.Client (Response (..))
import Network.Consensus.Raft.Timer (Microseconds, resetTimer)
import Network.Consensus.Raft.Transformer.Definition
import Network.Consensus.Raft.Transformer.Spec
import System.Random (uniformR)

dequeueEvent :: (MonadSTM m) => RaftT entry node state result message m (Event node entry result)
dequeueEvent = do
  queue <- view eventQueue
  lift $ atomically $ readTQueue queue

enqueueEvent :: (MonadSTM m) => Event node entry result -> RaftT entry node state result message m ()
enqueueEvent event = do
  queue <- view eventQueue
  lift $ atomically $ writeTQueue queue event

-- | Send a 'RPC' to another node.
--
-- To send a 'RPC' to multiple other nodes, concurrently, see 'sendRPCConcurrently'
sendRPC :: (Monad m) => node -> RPC node entry -> RaftT entry node state result message m ()
sendRPC node rpc = do
  spec <- view specification
  sendMessage node ((spec ^. serializeRPC) rpc)

-- | Send a 'RPC' concurrently to other nodes.
--
-- To send a 'RPC' to a single node, see 'sendRPC'
sendRPCConcurrently :: (MonadAsync m) => Set node -> RPC node entry -> RaftT entry node state result message m ()
sendRPCConcurrently nodes rpc = do
  spec <- view specification
  let !message = spec ^. serializeRPC $ rpc
  lift $ mapConcurrently_ (flip (spec ^. send) message) nodes

sendRPCResult :: (Monad m) => node -> RPCResult node result -> RaftT entry node state result message m ()
sendRPCResult node rpc = do
  spec <- view specification
  sendMessage node ((spec ^. serializeRPCResult) rpc)

sendClientResponse :: (Monad m) => node -> Response node result -> RaftT entry node state result message m ()
sendClientResponse node response = do
  spec <- view specification
  sendMessage node ((spec ^. serializeClientResponse) response)

sendMessage :: (Monad m) => node -> message -> RaftT entry node state result message m ()
sendMessage n m = do
  spec <- view specification
  lift $ (spec ^. send) n m

whenRole ::
  (Monad m) =>
  Role ->
  RaftT entry node state result message m () ->
  RaftT entry node state result message m ()
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
  RaftT entry node state result message m ()
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
sendAppendEntriesTo :: (Ord node, Monad m) => node -> RaftT entry node state result message m ()
sendAppendEntriesTo destination = do
  entries <- use logEntries
  (previousLogIndex, previousLogTerm) <-
    use (nextIndex . at destination) >>= \case
      Nothing -> pure (0, initialTerm)
      Just destNextIndex ->
        let previousLogIndex = pred destNextIndex
         in case Seq.lookup (fromIntegral previousLogIndex) entries of
              Just (prevTerm, _) -> pure (previousLogIndex, prevTerm)
              Nothing -> pure (0, initialTerm)

  let toBeReplicated = Seq.drop (fromIntegral previousLogIndex) entries
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

-- | Number of votes required for a decision to be majority.
quorum :: (Monad m) => RaftT entry node state result message m Int
quorum = do
  conf <- view configuration
  let numNodesInCluster = 1 + Set.size (otherNodes conf)
  pure $
    if even numNodesInCluster
      then numNodesInCluster `div` 2 + 1
      else (numNodesInCluster - 1) `div` 2 + 1

applyCommand :: (Monad m) => Command entry -> RaftT entry node state result message m (CommandResponse node result)
applyCommand (MkCommand entry requestId) = do
  apply <- view (specification . applyLogEntry)
  (newState, result) <- use internalState <&> flip apply entry
  trace (\n t -> LogEntryApplied n t entry)

  internalState .= newState
  pure (MkCommandResponse result requestId)

-- | Updates the commit index. Returns 'True' if some log entries can be applied,
-- and 'False' otherwise.
updateCommitIndex :: (Monad m) => RaftT entry node state result message m Bool
updateCommitIndex = do
  commitIndex' <- use commitIndex
  entries <- use logEntries
  ourTerm <- use term
  matchIndices <- use matchIndex
  quorumSize <- quorum

  -- Fetch the indices, starting from the log index after the commit index, such that:
  -- 1. the entry is associated with the current term
  -- 2. a quorum of nodes have committed this index
  let quorumIndices =
        Seq.zip
          ( Seq.fromFunction
              (Seq.length entries)
              -- The initial commit index is 0, hence the first entry to be committed
              -- should have index 1
              succ
          )
          entries
          & Seq.drop (fromIntegral commitIndex')
          & Seq.filter ((== ourTerm) . fst . snd)
          & fmap fst
          -- By definition of the Raft algorithm, only a contiguous subset of
          -- indices can have been accepted with a quorum, hence the use of
          -- takeWhileL
          & Seq.takeWhileL
            ( \i ->
                Map.size (Map.filter (>= fromIntegral i) matchIndices)
                  -- Note that 'matchIndices' do not include the leader,
                  -- so we take the leader into account by ensuring that there are
                  -- at least 'pred quorumSize' other nodes that have accepted the
                  -- entries, rather than 'quorumsize'
                  >= pred quorumSize
            )
          & fmap fromIntegral

  case Seq.viewr quorumIndices of
    Seq.EmptyR -> pure False
    _ :> lastIndex -> do
      commitIndex .= lastIndex
      trace (\n t -> CommitIndexIncreasedTo n t lastIndex)
      pure True

-- | Update the commit index, and apply log entries if there are
-- any log entries that /can/ be applied.
applyLogEntries :: (MonadMVar m) => RaftT entry node state result message m ()
applyLogEntries = do
  isTimeToCommit <- updateCommitIndex
  when isTimeToCommit $ do
    lastAppliedIndex <- use lastApplied
    currentCommitIndex <- use commitIndex
    unless (lastAppliedIndex == currentCommitIndex) $ do
      entries <- use logEntries
      let unAppliedEntries =
            entries
              & Seq.take (fromIntegral currentCommitIndex)
              & Seq.drop (fromIntegral lastAppliedIndex)
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

nextElectionTimeout :: (Monad m) => RaftT entry node state result message m Microseconds
nextElectionTimeout = do
  gen <- use randomGen
  (lowerBound, upperBound) <- view configuration <&> electionTimeoutRange
  let (timeout, nextGen) = uniformR (lowerBound, upperBound) gen
  randomGen .= nextGen
  pure timeout

trace :: (Monad m) => (Term -> node -> RaftTrace entry result node) -> RaftT entry node state result message m ()
trace makeTrace = do
  ourTerm <- use term
  node <- view configuration <&> nodeId
  spec <- view specification
  lift $ (spec ^. tracer) (makeTrace ourTerm node)

-- | Update the internal term state, returning the previous and new 'Term's.
--
-- If the update function returns the current term, this function does nothing.
updateTerm :: (Monad m) => (Term -> Term) -> RaftT entry node state result message m (Term, Term)
updateTerm update = do
  currTerm <- use term
  let newTerm = update currTerm

  when (newTerm > currTerm) $ do
    spec <- view specification

    lift $ (spec ^. writeTerm) newTerm
    term .= newTerm
  pure (currTerm, newTerm)

-- | Set up a callback for a client request
acceptClientRequest ::
  (MonadMVar m, MonadFork m) =>
  -- | Client node
  node ->
  RaftT entry node state result message m RequestId
acceptClientRequest clientId = do
  requests <- view currentClientRequests
  spec <- view specification
  let serializeResponse = view serializeClientResponse spec
      sendResponse = spec ^. send
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
          sendResponse clientId (serializeResponse (Success leaderId result))
      )
    $ \_ ->
      modifyMVar_ requests $
        pure . IntMap.delete (fromIntegral requestId)

  pure requestId
