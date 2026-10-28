{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TemplateHaskell #-}

module Distributed.Consensus.Raft.Admin
  ( RaftAdminT,
    AdminImplementation (..),
    withRaftAdminT,
    AdminError (..),
    Microseconds,

    -- * Available commands
    sendAdminCommand,

    -- ** Building commands
    leaveCluster,
    joinCluster,
    getClusterConfiguration,
    shutDown,

    -- * Communications between admins and clusters
    SessionId,
    sessionId,
    SequenceNum,
    AdminRequest,
    Request (..),
    AdminResponse,
    Response (..),
    AdminCommand (..),
    AdminCommandResult (..),
  )
where

import Control.Concurrent.Class.MonadSTM (MonadSTM, TMVar, TVar, atomically, newEmptyTMVar, newTVarIO, putTMVar, readTMVar, readTVar, writeTVar)
import Control.Monad.Class.MonadAsync (MonadAsync, withAsync)
import Control.Monad.Trans.Class (MonadTrans (lift))
import Control.Monad.Trans.Reader (ReaderT (runReaderT))
import qualified Control.Monad.Trans.Reader as Reader
import Data.Map (Map)
import qualified Data.Map.Strict as Map
import Distributed.Consensus.Raft.Domain (SequenceNum, SessionId, sessionId)
import Distributed.Consensus.Raft.Domain.Admin (AdminCommand (..), AdminCommandResult (..), AdminRequest, AdminRequestId (MkAdminRequestId), AdminResponse)
import Distributed.Consensus.Raft.Messaging (Request (..), Response (..))
import Distributed.Consensus.Raft.Timer (Microseconds)
import Lens.Micro.Platform (makeLenses)

newtype RaftAdminT node m a
  = MkRaftAdminT (ReaderT (RaftAdminEnv node m) m a)
  deriving (Functor, Applicative, Monad)

instance MonadTrans (RaftAdminT node) where
  lift = MkRaftAdminT . lift

asks :: (Monad m) => (RaftAdminEnv node m -> a) -> RaftAdminT node m a
asks = MkRaftAdminT . Reader.asks

data AdminImplementation node m = AdminImplementation
  { -- | Send an admin request to a node.
    sendAdminRequest :: node -> AdminRequest node -> m (),
    -- | Receive the next admin response.
    receiveAdminResponse :: m (AdminResponse node),
    -- | Generate a globally-unique session ID.
    generateSessionId :: node -> m SessionId
  }

-- | Open an admin session, and run an action with a runner for that session.
--
-- It is safe to use the runner from several threads concurrently.
withRaftAdminT ::
  (MonadAsync m) =>
  -- | self identification
  node ->
  AdminImplementation node m ->
  ((forall a. RaftAdminT node m a -> m a) -> m b) ->
  m b
withRaftAdminT self impl withSession = do
  nRId <- newTVarIO 0
  mbox <- newTVarIO mempty
  sessId <- generateSessionId impl self
  let recvLoop = do
        receiveAdminResponse impl >>= \resp -> do
          let adminReqId = responseRequestId resp
          atomically $ do
            box <- readTVar mbox
            case Map.lookup adminReqId box of
              Nothing -> pure ()
              Just var -> putTMVar var (responsePayload resp)
          recvLoop

  withAsync recvLoop $ \_ ->
    withSession
      ( \(MkRaftAdminT f) ->
          runReaderT
            f
            (MkRaftAdminEnv self sessId impl nRId mbox)
      )

data RaftAdminEnv node m
  = MkRaftAdminEnv
  { node :: !node,
    currentSessionId :: !SessionId,
    implementation :: AdminImplementation node m,
    nextRequestId :: TVar m SequenceNum,
    mailbox :: TVar m (Map AdminRequestId (TMVar m (AdminCommandResult node)))
  }

makeLenses ''AdminImplementation

data AdminError node
  = AdminNotLeader !(Maybe node)
  | UnexpectedAdminResponse
  deriving (Eq, Show)

newCommand ::
  (MonadSTM m) =>
  AdminCommand node ->
  RaftAdminT node m (AdminRequest node)
newCommand entry = do
  (seqNumVar, sessId, self) <- asks ((,,) <$> nextRequestId <*> currentSessionId <*> node)
  reqId <- lift $ atomically $ do
    nextSeqNum <- readTVar seqNumVar
    writeTVar seqNumVar (succ nextSeqNum)
    pure $ MkAdminRequestId sessId nextSeqNum

  pure $ MkRequest reqId self entry

joinCluster ::
  (MonadSTM m) =>
  -- | Leader of the cluster to join
  node ->
  RaftAdminT node m (AdminRequest node)
joinCluster = newCommand . JoinCluster

leaveCluster :: (MonadSTM m) => RaftAdminT node m (AdminRequest node)
leaveCluster = newCommand LeaveCluster

-- | Ask a node for the cluster configuration it has committed.
--
-- This is the only way for an admin to tell whether a 'joinCluster' or
-- 'leaveCluster' has actually taken effect.
getClusterConfiguration :: (MonadSTM m) => RaftAdminT node m (AdminRequest node)
getClusterConfiguration = newCommand GetClusterConfiguration

shutDown :: (MonadSTM m) => RaftAdminT node m (AdminRequest node)
shutDown = newCommand ShutDown

sendAdminCommand ::
  (MonadSTM m) =>
  node ->
  AdminRequest node ->
  RaftAdminT
    node
    m
    ( Either
        (AdminError node)
        (AdminCommandResult node)
    )
sendAdminCommand contact req@(MkRequest reqId _ command) = do
  send <- asks (sendAdminRequest . implementation)
  mbox <- asks mailbox
  lift $ do
    resultVar <- atomically $ do
      mbox' <- readTVar mbox
      resultVar <- newEmptyTMVar
      writeTVar mbox (Map.insert reqId resultVar mbox')

      pure resultVar

    send contact req

    -- TODO: clean up mailbox entry for req ID
    --       regardless of success (possibly using 'bracket')
    atomically
      ( do
          resp <- readTMVar resultVar
          mbox' <- readTVar mbox
          writeTVar mbox (Map.delete reqId mbox')
          pure (expectedResponse command resp)
      )

-- TODO: is there a better way to associate valid responses based on the request?
expectedResponse :: AdminCommand node -> AdminCommandResult node -> Either (AdminError node) (AdminCommandResult node)
expectedResponse _ (NotLeader leader) = Left (AdminNotLeader leader)
expectedResponse (JoinCluster {}) JoinInitiated = Right JoinInitiated
expectedResponse (LeaveCluster {}) LeaveInitiated = Right LeaveInitiated
expectedResponse GetClusterConfiguration resp@(ClusterConfigurationIs {}) = Right resp
expectedResponse ShutDown ShutdownInitiated = Right ShutdownInitiated
expectedResponse _ _ = Left UnexpectedAdminResponse
