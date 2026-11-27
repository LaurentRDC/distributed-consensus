{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TemplateHaskell #-}

-- |
-- Module:      $module
-- Copyright:   (c) Laurent P. René de Cotret
-- License:     BSD 3-Clause
--
-- Manage a Raft cluster as an administrator.
--
-- === How to use this module
--
-- The key piece of information to use this module effectively is that
-- interactions are done in /sessions/. An admin starts a session
-- by calling 'withAdminSession', and all requests will have to originate
-- from within.
--
-- Try to keep sessions open as long as possible -- ideally for the length
-- of an admin's lifetime.
--
-- Administrators create requests using 'leaveCluster', 'joinCluster', or other builders.
-- Requests are then sent using 'sendAdminCommand'. Interacting with a cluster may
-- require retrying or redirecting requests; reuse requests as much as possible.
-- To keep the implementation flexible, retrying and redirecting is something
-- you need to do yourself.
module Distributed.Consensus.Raft.Admin
  ( -- * Interface
    withAdminSession,
    RaftAdminT,

    -- ** Creating commands
    leaveCluster,
    joinCluster,
    getClusterConfiguration,
    shutDown,

    -- ** Sending commands
    sendAdminCommand,
    AdminError (..),

    -- ** Configuration
    AdminImplementation (..),

    -- * Other exports
    SessionId,
    sessionId,
    SequenceNum,
    Microseconds,
    AdminRequest,
    Request (..),
    AdminResponse,
    Response (..),
    AdminCommand (..),
    AdminCommandResult (..),
  )
where

import Control.Concurrent.Class.MonadSTM
  ( MonadSTM,
    TMVar,
    TVar,
    atomically,
    check,
    newEmptyTMVar,
    newTVarIO,
    orElse,
    readTMVar,
    readTVar,
    tryPutTMVar,
    writeTVar,
  )
import Control.Monad (void)
import Control.Monad.Class.MonadAsync (MonadAsync, link, withAsync)
import Control.Monad.Class.MonadFork (MonadFork)
import Control.Monad.Class.MonadThrow (MonadMask)
import Control.Monad.Class.MonadTimer (MonadTimer, registerDelay)
import Control.Monad.Trans.Class (MonadTrans (lift))
import Control.Monad.Trans.Reader (ReaderT (runReaderT))
import qualified Control.Monad.Trans.Reader as Reader
import Data.Map (Map)
import qualified Data.Map.Strict as Map
import Distributed.Consensus.Raft.Domain (SequenceNum, SessionId, sessionId)
import Distributed.Consensus.Raft.Domain.Admin
  ( AdminCommand (..),
    AdminCommandResult (..),
    AdminRequest,
    AdminRequestId (MkAdminRequestId),
    AdminResponse,
  )
import Distributed.Consensus.Raft.Messaging (Request (..), Response (..))
import Distributed.Consensus.Raft.Timer (Microseconds)
import Lens.Micro.Platform (makeLenses)

-- | Admin monad transformer to issue administrative commands to a Raft cluster.
--
-- The only way to execute an action of type 'RaftAdminT' is with
-- 'withAdminSession'.
newtype RaftAdminT node m a
  = MkRaftAdminT (ReaderT (RaftAdminEnv node m) m a)
  deriving (Functor, Applicative, Monad)

instance MonadTrans (RaftAdminT node) where
  lift = MkRaftAdminT . lift

asks :: (Monad m) => (RaftAdminEnv node m -> a) -> RaftAdminT node m a
asks = MkRaftAdminT . Reader.asks

-- | Implementation of communication between an administrator and a cluster.
--
-- See the @raft-consensus-distributed-process@ package for an example
-- implementation for the Cloud Haskell ecosystem.
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
--
-- The typical way you would use 'withAdminSession' is like so:
--
-- @
-- 'withAdminSession' nodeId $ \\run -> do
--     req <- run 'shutDown'
--
--     let go serverNode =
--             run ('sendAdminCommand' 1000000 serverNode req) >>= \case
--                Left _ -> go serverNode -- infinite retries
--                Right resp -> pure resp
--
--     go lastKnownLeader
-- @
--
-- In practice, 'withAdminSession' is usually run in its own thread,
-- and requests are sent over to that thread using e.g. a 'TQueue'.
withAdminSession ::
  ( MonadAsync m,
    MonadFork m,
    MonadMask m
  ) =>
  -- | self identification
  node ->
  AdminImplementation node m ->
  ((forall a. RaftAdminT node m a -> m a) -> m b) ->
  m b
withAdminSession self impl withSession = do
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
              -- Using 'tryPutTMVar' here in case of duplicate responses
              Just var -> void $ tryPutTMVar var (responsePayload resp)
          recvLoop

  withAsync recvLoop $ \thread -> do
    link thread
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
  | Timeout
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

-- | Command that a node join a Raft cluster.
--
-- This has no effect if the node is already a member of the
-- cluster.
joinCluster ::
  (MonadSTM m) =>
  -- | Leader of the cluster to join
  node ->
  RaftAdminT node m (AdminRequest node)
joinCluster = newCommand . JoinCluster

-- | Command that a node leave the Raft cluster.
--
-- Note that this node will not shut down unless the 'shutDown'
-- command is also sent to it.
leaveCluster :: (MonadSTM m) => RaftAdminT node m (AdminRequest node)
leaveCluster = newCommand LeaveCluster

-- | Ask a node for the cluster configuration it has committed.
--
-- This is the only way for an admin to tell whether a 'joinCluster' or
-- 'leaveCluster' has actually taken effect.
getClusterConfiguration :: (MonadSTM m) => RaftAdminT node m (AdminRequest node)
getClusterConfiguration = newCommand GetClusterConfiguration

-- | Request that a node shuts down.
shutDown :: (MonadSTM m) => RaftAdminT node m (AdminRequest node)
shutDown = newCommand ShutDown

sendAdminCommand ::
  (MonadTimer m) =>
  Microseconds ->
  node ->
  AdminRequest node ->
  RaftAdminT
    node
    m
    ( Either
        (AdminError node)
        (AdminCommandResult node)
    )
sendAdminCommand timeout contact req@(MkRequest reqId _ command) = do
  send <- asks (sendAdminRequest . implementation)
  mbox <- asks mailbox
  lift $ do
    resultVar <- atomically $ do
      mbox' <- readTVar mbox
      resultVar <- newEmptyTMVar
      writeTVar mbox (Map.insert reqId resultVar mbox')

      pure resultVar

    expired <- registerDelay (fromIntegral timeout)
    send contact req

    -- TODO: clean up mailbox entry for req ID
    --       regardless of success (possibly using 'bracket')
    atomically
      ( ( do
            resp <- readTMVar resultVar
            mbox' <- readTVar mbox
            writeTVar mbox (Map.delete reqId mbox')
            pure (expectedResponse command (Just resp))
        )
          `orElse` ( do
                       readTVar expired >>= check
                       readTVar mbox >>= writeTVar mbox . Map.delete reqId
                       pure (expectedResponse command Nothing)
                   )
      )

-- TODO: is there a better way to associate valid responses based on the request?
expectedResponse :: AdminCommand node -> Maybe (AdminCommandResult node) -> Either (AdminError node) (AdminCommandResult node)
expectedResponse _ Nothing = Left Timeout
expectedResponse _ (Just (NotLeader leader)) = Left (AdminNotLeader leader)
expectedResponse (JoinCluster {}) (Just JoinInitiated) = Right JoinInitiated
expectedResponse (LeaveCluster {}) (Just LeaveInitiated) = Right LeaveInitiated
expectedResponse GetClusterConfiguration (Just resp@(ClusterConfigurationIs {})) = Right resp
expectedResponse ShutDown (Just ShutdownInitiated) = Right ShutdownInitiated
expectedResponse _ _ = Left UnexpectedAdminResponse
