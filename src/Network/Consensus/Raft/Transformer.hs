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
    sendHeartbeat,
    sendAppendEntriesTo,
    quorum,
    applyLogEntries,
    nextElectionTimeout,
    updateTerm,
    trace,
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
import Data.Foldable (for_)
import qualified Data.Foldable as Foldable
import Data.Function ((&))
import Data.Functor ((<&>))
import qualified Data.Sequence as Seq
import Data.Set (Set)
import qualified Data.Set as Set
import qualified Data.Vector as Vector
import Lens.Micro.Platform (at, use, view, (.=), (^.))
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

sendMessage :: (Monad m) => node -> message -> RaftT entry node state result message m ()
sendMessage n m = do
  spec <- view specification
  lift $ (spec ^. send) n m

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
sendHeartbeat = do
  r <- use role
  when (r == Leader) $ do
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

applyCommand :: (Monad m) => Command node entry -> RaftT entry node state result message m (CommandResponse node result)
applyCommand (MkCommand sender entry requestId) = do
  apply <- view (specification . applyLogEntry)
  (newState, result) <- use internalState <&> flip apply entry
  trace (\n t -> LogEntryApplied n t entry)

  internalState .= newState
  pure (MkCommandResponse sender result requestId)

applyLogEntries :: (Monad m) => RaftT entry node state result message m ()
applyLogEntries = do
  lastAppliedIndex <- use lastApplied
  currentCommitIndex <- use commitIndex
  entries <- use logEntries
  let unAppliedEntries =
        entries
          & Seq.take (fromIntegral currentCommitIndex)
          & Seq.drop (fromIntegral lastAppliedIndex)
          & fmap snd

  results <- mapM applyCommand unAppliedEntries

  ourRole <- use role
  when (ourRole == Leader) $
    for_ results $
      \response@(MkCommandResponse client _ _) ->
        sendRPCResult client (ClientRequestResult response)

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
