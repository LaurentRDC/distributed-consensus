{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TemplateHaskell #-}

module Distributed.Consensus.Raft.Client
  ( RaftClientT,
    ClientImplementation (..),
    withClientSession,
    sendClientRequest,
    ClientError (..),
    Microseconds,

    -- ** Session information
    clientSession,

    -- ** Request payloads
    newRequest,

    -- * Communications between clients and clusters
    SessionId,
    sessionId,
    SequenceNum,
    ClientRequestId (..),
    ClientRequest,
    Request (..),
    ClientResponse,
    Response (..),
    ClientResult (..),
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
import Control.Monad.Trans.Class (MonadTrans, lift)
import Control.Monad.Trans.Reader (ReaderT (runReaderT), asks)
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.Semigroup (Max (..))
import Distributed.Consensus.Raft.Domain.Client
  ( ClientRequest,
    ClientRequestId (..),
    ClientResponse,
    ClientResult (..),
    SequenceNum,
    SessionId,
    sessionId,
  )
import Distributed.Consensus.Raft.Messaging (Request (..), Response (..))
import Distributed.Consensus.Raft.Timer (Microseconds)
import Lens.Micro.Platform (makeLenses)

-- | Client monad to make requests to a Raft cluster.
--
-- The only way to execute an action of type 'RaftClientT' is with
-- 'withClientSession'.
--
-- The only way to construct an action of type 'RaftClientT' is to use
-- 'request'.
--
-- If you must run computations in the base monad, use 'lift'.
newtype RaftClientT entry node result m a
  = MkRaftClientT (ReaderT (RaftClientEnv entry node result m) m a)
  deriving (Functor, Applicative, Monad)

instance MonadTrans (RaftClientT entry node result) where
  lift = MkRaftClientT . lift

data ClientImplementation entry node result m = ClientImplementation
  { sendRequest :: node -> ClientRequest entry node -> m (),
    receiveResponse :: m (ClientResponse node result),
    -- | Generate a globally-unique session ID.
    generateSessionId :: node -> m SessionId
  }

-- | Open a client session, and run an action with a runner for that session.
--
-- It is safe to use the runner from several threads concurrently.
withClientSession ::
  ( MonadAsync m,
    MonadFork m,
    MonadMask m
  ) =>
  -- | self identification
  node ->
  ClientImplementation entry node result m ->
  ((forall a. RaftClientT entry node result m a -> m a) -> m b) ->
  m b
withClientSession self impl withSession = do
  nRId <- newTVarIO 0
  lAck <- newTVarIO Nothing
  mbox <- newTVarIO mempty
  sessId <- generateSessionId impl self
  let env =
        MkRaftClientEnv
          { node = self,
            currentSessionId = sessId,
            nextRequestId = nRId,
            latestAcknowledgement = lAck,
            implementation = impl,
            mailbox = mbox
          }
      recvLoop = do
        receiveResponse impl >>= \resp -> do
          let reqId = responseRequestId resp
          atomically $ do
            box <- readTVar mbox
            case Map.lookup reqId box of
              Nothing -> pure ()
              -- Using 'tryPutTMVar' here in case a response is duplicated
              Just var -> void $ tryPutTMVar var resp
          recvLoop

  withAsync recvLoop $ \thread -> do
    link thread
    withSession (\(MkRaftClientT f) -> runReaderT f env)

data RaftClientEnv entry node result m
  = MkRaftClientEnv
  { node :: !node,
    currentSessionId :: !SessionId,
    nextRequestId :: !(TVar m SequenceNum),
    latestAcknowledgement :: !(TVar m (Maybe (Max SequenceNum))),
    implementation :: ClientImplementation entry node result m,
    mailbox :: TVar m (Map ClientRequestId (TMVar m (ClientResponse node result)))
  }

makeLenses ''ClientImplementation

-- | Possible client errors
data ClientError entry node
  = -- | The client should make the request to another node
    Redirect node
  | Timeout
  | NoKnownLeader
  deriving (Eq, Show, Ord)

-- | Read the current session ID. This helper can be useful if you need
-- to pin information to the current client
clientSession :: (Monad m) => RaftClientT entry node result m SessionId
clientSession = MkRaftClientT $ asks currentSessionId

newRequest :: (MonadSTM m) => entry -> RaftClientT entry node result m (ClientRequest entry node)
newRequest entry = do
  (seqNumVar, sessId, self) <- MkRaftClientT $ asks ((,,) <$> nextRequestId <*> currentSessionId <*> node)
  reqId <- lift $ atomically $ do
    nextSeqNum <- readTVar seqNumVar
    writeTVar seqNumVar (succ nextSeqNum)
    pure $ MkClientRequestId sessId nextSeqNum

  pure $ MkRequest reqId self entry

-- | Send a request to a Raft cluster.
--
-- The user must take care to handle the possible 'ClientError's, and be
-- ready to retry.
--
-- First, it is quite likely that the server will redirect requests
-- via @'Redirect' otherServer@. 'request' does not follow that redirect,
-- as you may want to control delays and retries.
--
-- Second, there is an unlikely, but real, possibility that the server
-- replies with 'NoKnownLeader'. In this case, you should try two things:
--
-- * Retry the request, which may resolve itself
-- * If retrying doesn't work, you should try to send the request to another server.
--   It may be beneficial to keep a memory of all servers ever contacted for this
--   particular purpose.
--
-- It is perfectly safe, and encouraged, to send separate requests in
-- separate threads. However, you should call 'withClientSession' only once.
sendClientRequest ::
  (MonadTimer m) =>
  -- | Maximum time to wait for a response.
  Microseconds ->
  node ->
  ClientRequest entry node ->
  RaftClientT
    entry
    node
    result
    m
    ( Either
        (ClientError entry node)
        (node, result)
    )
sendClientRequest timeout lastKnownLeader req@(MkRequest reqId _ _) = do
  send <- MkRaftClientT $ asks (sendRequest . implementation)
  mbox <- MkRaftClientT $ asks mailbox
  ackVar <- MkRaftClientT $ asks latestAcknowledgement
  lift
    ( do
        resultVar <- atomically $ do
          mbox' <- readTVar mbox
          resultVar <- newEmptyTMVar
          -- TODO: bracket to deregister in case of an exception
          writeTVar mbox (Map.insert reqId resultVar mbox')

          pure resultVar

        expired <- registerDelay (fromIntegral timeout)
        send lastKnownLeader req

        -- TODO: clean up mailbox entry for req ID
        --       regardless of success (possibly using 'bracket')
        atomically
          ( ( do
                resp <- readTMVar resultVar
                mbox' <- readTVar mbox
                lAck <- readTVar ackVar
                writeTVar mbox (Map.delete reqId mbox')

                let MkClientRequestId _ respSeqNum = responseRequestId resp
                    watermark = Max respSeqNum
                writeTVar
                  ackVar
                  ( maybe
                      (Just watermark)
                      (Just . mappend watermark)
                      lAck
                  )

                pure (Just resp)
            )
              `orElse` ( do
                           readTVar expired >>= check
                           readTVar mbox >>= writeTVar mbox . Map.delete reqId
                           pure Nothing
                       )
          )
    )
    >>= \case
      Nothing -> pure $ Left Timeout
      Just (MkResponse _ (Just actualLeaderId) NotLeader) -> pure $ Left $ Redirect actualLeaderId
      Just (MkResponse _ (Just leader) (Success result)) -> pure $ Right (leader, result)
      -- TODO: what if leader is @Nothing@ but response is `Success`??
      Just (MkResponse _ Nothing _) -> pure $ Left NoKnownLeader
