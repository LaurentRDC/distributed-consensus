{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TemplateHaskell #-}

module Distributed.Consensus.Raft.Admin
  ( RaftAdminT,
    RaftAdminSpec (..),
    withRaftAdminT,

    -- * Available commands
    joinCluster,
    leaveCluster,
    getClusterConfiguration,
    shutDown,

    -- * Communications between admins and clusters
    AdminRequest,
    AdminResponse,
    AdminCommand (..),
    AdminCommandResult (..),
    AdminError (..),
  )
where

import Control.Concurrent.Class.MonadSTM (TMVar, TVar, atomically, newEmptyTMVar, newTVarIO, putTMVar, readTMVar, readTVar, writeTVar)
import Control.Monad.Class.MonadAsync (MonadAsync, withAsync)
import Control.Monad.Class.MonadSTM (MonadSTM)
import Control.Monad.Trans.Class (MonadTrans (lift))
import Control.Monad.Trans.Reader (ReaderT (runReaderT))
import qualified Control.Monad.Trans.Reader as Reader
import Data.Binary (Binary)
import Data.Map (Map)
import qualified Data.Map.Strict as Map
import Data.Text (Text)
import Data.Word (Word64)
import Distributed.Consensus.Raft.Domain (ClusterConfiguration)
import Distributed.Consensus.Raft.Messaging (Request (..), Response (..))
import GHC.Generics (Generic)
import Lens.Micro.Platform (makeLenses)

data AdminCommand node
  = JoinCluster
      -- | Target node
      node
  | LeaveCluster
  | -- | Ask a node for the cluster configuration it has committed.
    GetClusterConfiguration
  | ShutDown
  deriving (Eq, Show, Ord, Generic)

instance (Binary node) => Binary (AdminCommand node)

newtype AdminRequestId = AdminRequestId Word64
  deriving stock (Generic, Eq, Ord, Show)
  deriving newtype (Real, Binary, Enum, Num, Integral)

type AdminRequest node = Request AdminRequestId node (AdminCommand node)

data AdminCommandResult node
  = JoinInitiated
  | LeaveInitiated
  | ShutdownInitiated
  | ClusterConfigurationIs !(ClusterConfiguration node)
  | -- | The command could not be completed.
    AdminFailure !Text
  | -- | The contacted node isn't the leader. The 'Maybe' contains the
    -- | node believed to be the current leader, if known.
    NotLeader !(Maybe node)
  deriving (Eq, Show, Generic)

instance (Binary node) => Binary (AdminCommandResult node)

type AdminResponse node = Response AdminRequestId node (AdminCommandResult node)

newtype RaftAdminT node m a
  = MkRaftAdminT (ReaderT (RaftAdminEnv node m) m a)
  deriving (Functor, Applicative, Monad)

instance MonadTrans (RaftAdminT node) where
  lift = MkRaftAdminT . lift

asks :: (Monad m) => (RaftAdminEnv node m -> a) -> RaftAdminT node m a
asks = MkRaftAdminT . Reader.asks

data RaftAdminSpec node m = MkRaftAdminSpec
  { -- | Send an admin request to a node.
    sendAdminRequest ::
      node ->
      AdminRequest node ->
      m (),
    -- | Receive the next admin response.
    receiveAdminResponse ::
      m (Either Text (AdminResponse node))
  }

-- | Open an admin session, and run an action with a runner for that session.
--
-- It is safe to use the runner from several threads concurrently.
withRaftAdminT ::
  (MonadAsync m) =>
  -- | self identification
  node ->
  RaftAdminSpec node m ->
  ((forall a. RaftAdminT node m a -> m a) -> m b) ->
  m b
withRaftAdminT self impl withSession = do
  nRId <- newTVarIO 0
  mbox <- newTVarIO mempty

  let recvLoop = do
        receiveAdminResponse impl >>= \case
          Left _ -> recvLoop
          Right resp -> do
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
    implementation :: RaftAdminSpec node m,
    nextRequestId :: TVar m AdminRequestId,
    mailbox :: TVar m (Map AdminRequestId (TMVar m (AdminCommandResult node)))
  }

makeLenses ''RaftAdminSpec

data AdminError node
  = AdminFailed !Text
  | AdminNotLeader !(Maybe node)
  | UnexpectedAdminResponse
  deriving (Eq, Show)

sendAdminCommand :: (MonadSTM m) => node -> AdminCommand node -> RaftAdminT node m (AdminCommandResult node)
sendAdminCommand contact command = do
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

    atomically
      ( do
          resp <- readTMVar resultVar
          mbox' <- readTVar mbox
          writeTVar mbox (Map.delete rid mbox')
          pure resp
      )

joinCluster ::
  (MonadSTM m) =>
  -- | Node to command
  node ->
  -- \| Node to join
  node ->
  RaftAdminT node m (Either (AdminError node) ())
joinCluster contact target = do
  sendAdminCommand contact (JoinCluster target)
    >>= \case
      JoinInitiated ->
        pure (Right ())
      AdminFailure err ->
        pure (Left (AdminFailed err))
      NotLeader leader ->
        pure (Left (AdminNotLeader leader))
      _ ->
        pure (Left UnexpectedAdminResponse)

leaveCluster ::
  (MonadSTM m) =>
  -- | Node to command
  node ->
  RaftAdminT node m (Either (AdminError node) ())
leaveCluster contact = do
  sendAdminCommand contact LeaveCluster
    >>= \case
      LeaveInitiated ->
        pure (Right ())
      AdminFailure err ->
        pure (Left (AdminFailed err))
      NotLeader leader ->
        pure (Left (AdminNotLeader leader))
      _ ->
        pure (Left UnexpectedAdminResponse)

-- | Ask a node for the cluster configuration it has committed.
--
-- This is the only way for an admin to tell whether a 'joinCluster' or
-- 'leaveCluster' has actually taken effect.
getClusterConfiguration ::
  (MonadSTM m) =>
  -- | Node to ask.
  node ->
  RaftAdminT node m (Either (AdminError node) (ClusterConfiguration node))
getClusterConfiguration contact = do
  sendAdminCommand contact GetClusterConfiguration
    >>= \case
      ClusterConfigurationIs conf ->
        pure (Right conf)
      AdminFailure err ->
        pure (Left (AdminFailed err))
      NotLeader leader ->
        pure (Left (AdminNotLeader leader))
      _ ->
        pure (Left UnexpectedAdminResponse)

shutDown ::
  (MonadSTM m) =>
  -- | Node to command
  node ->
  RaftAdminT node m (Either (AdminError node) ())
shutDown contact = do
  sendAdminCommand contact ShutDown
    >>= \case
      ShutdownInitiated ->
        pure (Right ())
      AdminFailure err ->
        pure (Left (AdminFailed err))
      NotLeader leader ->
        pure (Left (AdminNotLeader leader))
      _ ->
        pure (Left UnexpectedAdminResponse)
