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
import qualified Data.Sequence as Seq
import qualified Data.Set as Set
import Lens.Micro.Platform (use, view, (%=), (+=), (.=), (^.))
import Network.Consensus.Raft.Spec
  ( LogIndex,
    RPC (RequestVote),
    RPCResult (RequestVoteResult),
    RaftTrace (..),
    Role (..),
    Term,
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
    sendRPCResult,
    specification,
    trace,
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
  when (r /= Leader) becomeCandidate
handleEvent EventHeartBeatTimeout = do
  config <- view configuration
  view heartBeatTimer >>= lift . resetTimer config.heartBeatTimeout
handleEvent (EventRPC (RequestVote candidateTerm candidateNode candidateLastLogEntry candidateLastLogEntryTerm)) = handleRequestVote candidateTerm candidateNode candidateLastLogEntry candidateLastLogEntryTerm
handleEvent (EventRPC rpc) = undefined
handleEvent (EventRPCResult (RequestVoteResult voter voterTerm votedForUs)) = handleRequestVoteResult voter voterTerm votedForUs
handleEvent (EventRPCResult result) = undefined

-- | How to handle a term provided by another node. If this term
-- is larger than ours, this means that we must clear some state from
-- the previous term.
handleTermNumber ::
  ( MonadAsync m,
    MonadMask m,
    MonadFork m,
    MonadMVar m,
    MonadDelay m
  ) =>
  Term -> RaftT entry node result message m ()
handleTermNumber newTerm = do
  ourTerm <- use term
  when (newTerm > ourTerm) $ do
    spec <- view specification

    lift $ (spec ^. writeTerm) newTerm
    term .= newTerm
    votedFor .= Nothing
    becomeFollower

handleRequestVote ::
  ( Eq node,
    MonadAsync m,
    MonadMask m,
    MonadFork m,
    MonadMVar m,
    MonadDelay m
  ) =>
  Term -> node -> LogIndex -> Term -> RaftT entry node result message m ()
handleRequestVote candidateTerm candidateNode candidateLastLogIndex candidateLastLogIndexTerm = do
  handleTermNumber candidateTerm

  mAlreadyVoted <- use votedFor
  self <- view configuration <&> nodeId
  entries <- use logEntries
  ourTerm <- use term
  case mAlreadyVoted of
    -- In any situation, if the candidate's term is old,
    -- we should not vote for them
    _
      | candidateTerm < ourTerm ->
          sendRPCResult candidateNode (RequestVoteResult self ourTerm False)
    -- We haven't voted yet
    Nothing ->
      if (candidateLastLogIndex, candidateLastLogIndexTerm) >= lastLogInfo entries
        then do
          votedFor .= Just candidateNode
          sendRPCResult candidateNode (RequestVoteResult self ourTerm True)
        else
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
  handleTermNumber voterTerm
  ourRole <- use role
  when (ourRole == Candidate) $
    when votedForUs $ do
      yesVotes %= Set.insert voter
      checkElection

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
  trace (LeaderElected self)

  -- TODO: send append all entries messages to all followers

  config <- view configuration
  view heartBeatTimer >>= lift . resetTimer config.heartBeatTimeout

becomeFollower ::
  ( MonadAsync m,
    MonadMask m,
    MonadFork m,
    MonadMVar m,
    MonadDelay m
  ) =>
  RaftT entry node result message m ()
becomeFollower = do
  role .= Follower
  et <- view electionTimer
  electionTimeout <- nextElectionTimeout
  lift $ resetTimer electionTimeout et
