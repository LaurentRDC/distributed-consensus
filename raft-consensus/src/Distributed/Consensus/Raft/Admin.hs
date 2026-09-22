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
    joinCluster,
    leaveCluster,
    getClusterConfiguration,
    shutDown,

    -- * Communications between admins and clusters
    AdminRequest,
    Request (..),
    AdminResponse,
    Response (..),
    AdminCommand (..),
    AdminCommandResult (..),
  )
where

import Control.Concurrent.Class.MonadSTM (TMVar, TVar, atomically, newEmptyTMVar, newTVarIO, putTMVar, readTMVar, readTVar, writeTVar)
import Control.Monad.Class.MonadAsync (MonadAsync, withAsync)
import Control.Monad.Class.MonadTimer (MonadTimer, timeout)
import Control.Monad.Trans.Class (MonadTrans (lift))
import Control.Monad.Trans.Reader (ReaderT (runReaderT))
import qualified Control.Monad.Trans.Reader as Reader
import Data.Map (Map)
import qualified Data.Map.Strict as Map
import Data.Text (Text)
import Distributed.Consensus.Raft.Domain (ClusterConfiguration)
import Distributed.Consensus.Raft.Domain.Admin (AdminCommand (..), AdminCommandResult (..), AdminRequest, AdminRequestId, AdminResponse)
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
    sendAdminRequest ::
      node ->
      AdminRequest node ->
      m (),
    -- | Receive the next admin response.
    receiveAdminResponse ::
      m (AdminResponse node)
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
    withSession (\(MkRaftAdminT f) -> runReaderT f (MkRaftAdminEnv self impl nRId mbox))

data RaftAdminEnv node m
  = MkRaftAdminEnv
  { node :: !node,
    implementation :: AdminImplementation node m,
    nextRequestId :: TVar m AdminRequestId,
    mailbox :: TVar m (Map AdminRequestId (TMVar m (AdminCommandResult node)))
  }

makeLenses ''AdminImplementation

data AdminError node
  = AdminFailed !Text
  | AdminNotLeader !(Maybe node)
  | Timeout
  | UnexpectedAdminResponse
  deriving (Eq, Show)

sendAdminCommand ::
  (MonadTimer m) =>
  node ->
  AdminCommand node ->
  -- | Timeout in microseconds
  Microseconds ->
  RaftAdminT node m (Maybe (AdminCommandResult node))
sendAdminCommand contact command timeoutValue = do
  send <- asks (sendAdminRequest . implementation)
  admin <- asks node
  mbox <- asks mailbox
  reqIdVar <- asks nextRequestId
  lift $ do
    (rid, resultVar) <- atomically $ do
      r <- readTVar reqIdVar
      writeTVar reqIdVar (succ r)

      mbox' <- readTVar mbox
      resultVar <- newEmptyTMVar
      writeTVar mbox (Map.insert r resultVar mbox')

      pure (r, resultVar)

    send
      contact
      MkRequest
        { requestId = rid,
          requestOriginator = admin,
          requestPayload = command
        }
    timeout (fromIntegral timeoutValue) $
      atomically
        ( do
            resp <- readTMVar resultVar
            mbox' <- readTVar mbox
            writeTVar mbox (Map.delete rid mbox')
            pure resp
        )

joinCluster ::
  (MonadTimer m) =>
  -- | Timeout in microseconds
  Microseconds ->
  -- | Node to command
  node ->
  -- \| Node to join
  node ->
  RaftAdminT node m (Either (AdminError node) ())
joinCluster timeoutValue contact target = do
  sendAdminCommand contact (JoinCluster target) timeoutValue
    >>= \case
      Just JoinInitiated ->
        pure (Right ())
      Just (AdminFailure err) ->
        pure (Left (AdminFailed err))
      Just (NotLeader leader) ->
        pure (Left (AdminNotLeader leader))
      Just _ ->
        pure (Left UnexpectedAdminResponse)
      Nothing ->
        pure (Left Timeout)

leaveCluster ::
  (MonadTimer m) =>
  -- | Timeout in microseconds
  Microseconds ->
  -- | Node to command
  node ->
  RaftAdminT node m (Either (AdminError node) ())
leaveCluster timeoutValue contact = do
  sendAdminCommand contact LeaveCluster timeoutValue
    >>= \case
      Just LeaveInitiated ->
        pure (Right ())
      Just (AdminFailure err) ->
        pure (Left (AdminFailed err))
      Just (NotLeader leader) ->
        pure (Left (AdminNotLeader leader))
      Just _ ->
        pure (Left UnexpectedAdminResponse)
      Nothing ->
        pure (Left Timeout)

-- | Ask a node for the cluster configuration it has committed.
--
-- This is the only way for an admin to tell whether a 'joinCluster' or
-- 'leaveCluster' has actually taken effect.
getClusterConfiguration ::
  (MonadTimer m) =>
  -- | Timeout in microseconds
  Microseconds ->
  -- | Node to ask.
  node ->
  RaftAdminT node m (Either (AdminError node) (ClusterConfiguration node))
getClusterConfiguration timeoutValue contact = do
  sendAdminCommand contact GetClusterConfiguration timeoutValue
    >>= \case
      Just (ClusterConfigurationIs conf) ->
        pure (Right conf)
      Just (AdminFailure err) ->
        pure (Left (AdminFailed err))
      Just (NotLeader leader) ->
        pure (Left (AdminNotLeader leader))
      Just _ ->
        pure (Left UnexpectedAdminResponse)
      Nothing ->
        pure (Left Timeout)

shutDown ::
  (MonadTimer m) =>
  -- | Timeout in microseconds
  Microseconds ->
  -- | Node to command
  node ->
  RaftAdminT node m (Either (AdminError node) ())
shutDown timeoutValue contact = do
  sendAdminCommand contact ShutDown timeoutValue
    >>= \case
      Just ShutdownInitiated ->
        pure (Right ())
      Just (AdminFailure err) ->
        pure (Left (AdminFailed err))
      Just (NotLeader leader) ->
        pure (Left (AdminNotLeader leader))
      Just _ ->
        pure (Left UnexpectedAdminResponse)
      Nothing ->
        pure (Left Timeout)
