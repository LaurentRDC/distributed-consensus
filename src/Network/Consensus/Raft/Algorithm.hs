{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Network.Consensus.Raft.Algorithm (server) where

import Control.Concurrent.Class.MonadMVar (MonadMVar)
import Control.Concurrent.Class.MonadSTM (atomically, writeTQueue)
import Control.Monad (forever, when)
import Control.Monad.Class.MonadAsync (MonadAsync)
import Control.Monad.Class.MonadFork (MonadFork, forkIO, labelThisThread)
import Control.Monad.Class.MonadThrow (MonadMask)
import Control.Monad.Class.MonadTimer (MonadDelay)
import Control.Monad.Trans.Class (lift)
import Data.Functor ((<&>))
import Data.Sequence (Seq (..))
import qualified Data.Set as Set
import Lens.Micro.Platform (use, view, (+=), (.=), (^.))
import Network.Consensus.Raft.Spec
  ( RPC (RequestVote),
    RaftTrace (..),
    Role (..),
    currentLeader,
    deserializeRPC,
    deserializeRPCResult,
    lastApplied,
    logEntries,
    receive,
    role,
    term,
    voteFor,
    votedFor,
    writeTerm,
    yesVotes,
  )
import Network.Consensus.Raft.Timer (resetTimer)
import Network.Consensus.Raft.Trans
  ( Config (..),
    Event (..),
    RaftT,
    configuration,
    dequeueEvent,
    electionTimer,
    eventQueue,
    heartBeatTimer,
    nextElectionTimeout,
    quorum,
    sendRPC,
    specification,
    trace,
  )

server ::
  ( MonadMask m,
    MonadFork m,
    MonadMVar m,
    MonadAsync m,
    MonadDelay m
  ) =>
  RaftT entry node result message m ()
server = do
  spec <- view specification
  queue <- view eventQueue
  _ <- lift $ forkIO (receiveMessages spec queue)

  config <- view configuration
  view heartBeatTimer >>= lift . resetTimer config.heartBeatTimeout
  electionTimeout <- nextElectionTimeout
  view electionTimer >>= lift . resetTimer electionTimeout
  forever (dequeueEvent >>= handleEvent)
  where
    receiveMessages spec queue = do
      labelThisThread "receiveMessages"
      let decodeRPC = spec ^. deserializeRPC
          decodeRPCResult = spec ^. deserializeRPCResult
          recv = spec ^. receive
      forever $ do
        message <- recv
        case decodeRPC message of
          Just rpc -> atomically $ writeTQueue queue (EventRPC rpc)
          Nothing -> case decodeRPCResult message of
            Just result -> atomically $ writeTQueue queue (EventRPCResult result)
            Nothing -> pure () -- TODO: debug error message

handleEvent ::
  ( MonadDelay m,
    MonadMask m,
    MonadFork m,
    MonadAsync m,
    MonadMVar m
  ) =>
  Event node entry result -> RaftT entry node result message m ()
handleEvent EventElectionTimeout = do
  r <- use role
  when (r /= Leader) becomeCandidate
handleEvent EventHeartBeatTimeout = do
  config <- view configuration
  view heartBeatTimer >>= lift . resetTimer config.heartBeatTimeout
handleEvent (EventRPC rpc) = undefined
handleEvent (EventRPCResult result) = undefined

becomeCandidate ::
  ( MonadAsync m,
    MonadMask m,
    MonadFork m,
    MonadMVar m,
    MonadDelay m
  ) =>
  RaftT entry node result message m ()
becomeCandidate = do
  role .= Candidate

  term += 1
  w <- view (specification . writeTerm)
  use term >>= lift . w

  self <- view configuration <&> nodeId
  v <- view (specification . voteFor)
  lift $ v (Just self)
  votedFor .= Just self
  yesVotes .= Set.singleton self

  et <- view electionTimer
  electionTimeout <- nextElectionTimeout
  lift $ resetTimer electionTimeout et

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
    mapM_ (`sendRPC` rpc) peers -- TODO: send concurrently
  where
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
  trace (LeaderElected self)

  -- TODO: send append all entries messages to all followers

  config <- view configuration
  view heartBeatTimer >>= lift . resetTimer config.heartBeatTimeout
