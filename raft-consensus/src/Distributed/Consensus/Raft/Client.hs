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
    request,
    ClientError (..),
    Microseconds,

    -- * Communications between clients and clusters
    ClientRequestId (..),
    ClientRequest,
    Request (..),
    ClientResponse,
    Response (..),
    ClientResult (..),
  )
where

import Control.Arrow ((&&&))
import Control.Concurrent.Class.MonadSTM (TMVar, TVar, atomically, newEmptyTMVar, newTVarIO, putTMVar, readTMVar, readTVar, writeTVar)
import Control.Monad.Class.MonadAsync (MonadAsync, withAsync)
import Control.Monad.Class.MonadTimer (MonadTimer, timeout)
import Control.Monad.Trans.Class (MonadTrans, lift)
import Control.Monad.Trans.Reader (ReaderT (runReaderT), asks)
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.Text (Text)
import Distributed.Consensus.Raft.Domain.Client (ClientRequest, ClientRequestId (..), ClientResponse, ClientResult (..))
import Distributed.Consensus.Raft.Messaging (Request (..), Response (..))
import Distributed.Consensus.Raft.Timer (Microseconds)
import Lens.Micro.Platform (makeLenses)

newtype RaftClientT entry node result m a
  = MkRaftClientT (ReaderT (RaftClientEnv entry node result m) m a)
  deriving (Functor, Applicative, Monad)

instance MonadTrans (RaftClientT entry node result) where
  lift = MkRaftClientT . lift

data ClientImplementation entry node result m = ClientImplementation
  { sendRequest :: node -> ClientRequest node entry -> m (),
    receiveResponse :: m (ClientResponse node result)
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
  let env =
        MkRaftClientEnv
          { node = self,
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
    nextRequestId :: !(TVar m ClientRequestId),
    implementation :: ClientImplementation entry node result m,
    mailbox :: TVar m (Map ClientRequestId (TMVar m (ClientResponse node result)))
  }

makeLenses ''ClientImplementation

-- | Possible client errors
data ClientError
  = Timeout
  | NoKnownLeader
  | SomeFailure !Text
  deriving (Eq, Show, Ord)

-- | Send a request to a Raft cluster.
--
-- It is perfectly safe, and encouraged, to send separate requests in
-- separate threads.
request ::
  (MonadTimer m) =>
  -- | Timeout in microseconds
  Microseconds ->
  node ->
  entry ->
  RaftClientT entry node result m (Either ClientError (node, result))
request timeoutValue lastKnownLeader entry = do
  (self, send) <- MkRaftClientT $ asks (node &&& sendRequest . implementation)
  mbox <- MkRaftClientT $ asks mailbox
  reqIdVar <- MkRaftClientT $ asks nextRequestId
  lift
    ( do
        (reqId, resultVar) <- atomically $ do
          r <- readTVar reqIdVar
          writeTVar reqIdVar (succ r)

          mbox' <- readTVar mbox
          resultVar <- newEmptyTMVar
          writeTVar mbox (Map.insert r resultVar mbox')

          pure (r, resultVar)

        send
          lastKnownLeader
          (MkRequest reqId self entry)

        timeout (fromIntegral timeoutValue) $
          atomically
            ( do
                resp <- readTMVar resultVar
                mbox' <- readTVar mbox
                writeTVar mbox (Map.delete reqId mbox')
                pure resp
            )
    )
    >>= \case
      Just (MkResponse _ _ (Failure errmsg)) -> pure $ Left $ SomeFailure errmsg
      Just (MkResponse _ (Just actualLeaderId) NotLeader) -> request timeoutValue actualLeaderId entry
      Just (MkResponse _ (Just leader) (Success result)) -> pure $ Right (leader, result)
      -- TODO: what if leader is @Nothing@ but response is `Success`??
      Just (MkResponse _ Nothing _) -> pure $ Left NoKnownLeader
      Nothing -> pure $ Left Timeout
