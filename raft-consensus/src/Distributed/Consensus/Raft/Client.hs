{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TemplateHaskell #-}

module Distributed.Consensus.Raft.Client
  ( RaftClientT,
    ClientImplementation (..),
    withRaftClientT,
    sendClientRequest,
    ClientError (..),
    Microseconds,

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

import Control.Concurrent.Class.MonadSTM (MonadSTM, TMVar, TVar, atomically, newEmptyTMVar, newTVarIO, putTMVar, readTMVar, readTVar, writeTVar)
import Control.Monad.Class.MonadAsync (MonadAsync, withAsync)
import Control.Monad.Trans.Class (MonadTrans, lift)
import Control.Monad.Trans.Reader (ReaderT (runReaderT), asks)
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
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
-- 'withRaftClientT'.
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
  { sendRequest :: node -> ClientRequest node entry -> m (),
    receiveResponse :: m (ClientResponse node result),
    -- | Generate a globally-unique session ID.
    generateSessionId :: node -> m SessionId
  }

-- | Open a client session, and run an action with a runner for that session.
--
-- It is safe to use the runner from several threads concurrently.
withRaftClientT ::
  (MonadAsync m) =>
  -- | self identification
  node ->
  ClientImplementation entry node result m ->
  ((forall a. RaftClientT entry node result m a -> m a) -> m b) ->
  m b
withRaftClientT self impl withSession = do
  nRId <- newTVarIO 0
  mbox <- newTVarIO mempty
  sessId <- generateSessionId impl self
  let env =
        MkRaftClientEnv
          { node = self,
            currentSessionId = sessId,
            nextRequestId = nRId,
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
              Just var -> putTMVar var resp
          recvLoop

  withAsync recvLoop $ \_ ->
    withSession (\(MkRaftClientT f) -> runReaderT f env)

data RaftClientEnv entry node result m
  = MkRaftClientEnv
  { node :: !node,
    currentSessionId :: !SessionId,
    nextRequestId :: !(TVar m SequenceNum),
    implementation :: ClientImplementation entry node result m,
    mailbox :: TVar m (Map ClientRequestId (TMVar m (ClientResponse node result)))
  }

makeLenses ''ClientImplementation

-- | Possible client errors
data ClientError entry node
  = -- | The client should make the request to another node
    Redirect node
  | NoKnownLeader
  deriving (Eq, Show, Ord)

newRequest :: (MonadSTM m) => entry -> RaftClientT entry node result m (ClientRequest node entry)
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
-- separate threads. However, you should call 'withRaftClientT' only once.
sendClientRequest ::
  (MonadSTM m) =>
  node ->
  ClientRequest node entry ->
  RaftClientT
    entry
    node
    result
    m
    ( Either
        (ClientError entry node)
        (node, result)
    )
sendClientRequest lastKnownLeader req@(MkRequest reqId _ _) = do
  send <- MkRaftClientT $ asks (sendRequest . implementation)
  mbox <- MkRaftClientT $ asks mailbox
  lift
    ( do
        resultVar <- atomically $ do
          mbox' <- readTVar mbox
          resultVar <- newEmptyTMVar
          -- TODO: bracket to deregister
          writeTVar mbox (Map.insert reqId resultVar mbox')

          pure resultVar

        send lastKnownLeader req

        -- TODO: clean up mailbox entry for req ID
        --       regardless of success (possibly using 'bracket')
        atomically
          ( do
              resp <- readTMVar resultVar
              mbox' <- readTVar mbox
              writeTVar mbox (Map.delete reqId mbox')
              pure resp
          )
    )
    >>= \case
      (MkResponse _ (Just actualLeaderId) NotLeader) -> pure $ Left $ Redirect actualLeaderId
      (MkResponse _ (Just leader) (Success result)) -> pure $ Right (leader, result)
      -- TODO: what if leader is @Nothing@ but response is `Success`??
      (MkResponse _ Nothing _) -> pure $ Left NoKnownLeader
