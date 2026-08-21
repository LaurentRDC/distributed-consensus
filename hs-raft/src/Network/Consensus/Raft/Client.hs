{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TemplateHaskell #-}

module Network.Consensus.Raft.Client
  ( RaftClientT,
    RaftClientSpec (..),
    withRaftClientT,
    request,

    -- * Communications between clients and clusters
    ClientRequestId (..),
    ClientRequest,
    ClientResponse,
    ClientResult (..),
  )
where

import Control.Arrow ((&&&))
import Control.Concurrent.Class.MonadSTM (MonadSTM, TMVar, TVar, atomically, newEmptyTMVar, newTVarIO, putTMVar, readTMVar, readTVar, writeTVar)
import Control.Monad.Class.MonadAsync (MonadAsync, withAsync)
import Control.Monad.Trans.Class (MonadTrans, lift)
import Control.Monad.Trans.Reader (ReaderT (runReaderT), asks)
import Data.Binary (Binary)
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.Text (Text)
import Data.Word (Word64)
import GHC.Generics (Generic)
import Lens.Micro.Platform (makeLenses)
import Network.Consensus.Raft.Messaging (Request (..), Response (..))

-- Alphabet for communicating with clients

-- | Client-sourced request ID. This allows to correlate multiple client
-- side-responses.
newtype ClientRequestId = ClientRequestId Word64
  deriving stock (Generic, Eq, Ord, Show)
  deriving newtype (Real, Binary, Enum, Num, Integral)

type ClientRequest node entry = Request ClientRequestId node entry

data ClientResult node result
  = Success !result
  | Failure !Text
  | NotLeader
  deriving (Eq, Show, Generic)

instance (Binary node, Binary result) => Binary (ClientResult node result)

type ClientResponse node result = Response ClientRequestId node (ClientResult node result)

newtype RaftClientT entry node result m a
  = MkRaftClientT (ReaderT (RaftClientEnv entry node result m) m a)
  deriving (Functor, Applicative, Monad)

instance MonadTrans (RaftClientT entry node result) where
  lift = MkRaftClientT . lift

data RaftClientSpec entry node result m = MkRaftClientSpec
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
  RaftClientSpec entry node result m ->
  ((forall a. RaftClientT entry node result m a -> m a) -> m b) ->
  m b
withRaftClientT self spec withSession = do
  nRId <- newTVarIO 0
  mbox <- newTVarIO mempty
  let env =
        MkRaftClientEnv
          { node = self,
            nextRequestId = nRId,
            specification = spec,
            mailbox = mbox
          }
      recvLoop = do
        receiveResponse spec >>= \resp -> do
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
    specification :: RaftClientSpec entry node result m,
    mailbox :: TVar m (Map ClientRequestId (TMVar m (ClientResponse node result)))
  }

makeLenses ''RaftClientSpec

-- | Send a request to a Raft cluster.
--
-- It is perfectly safe, and encouraged, to send separate requests in
-- separate threads.
request ::
  (MonadSTM m) =>
  node ->
  entry ->
  RaftClientT entry node result m (Either Text (node, result))
request lastKnownLeader entry = do
  (self, send) <- MkRaftClientT $ asks (node &&& sendRequest . specification)
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

        atomically
          ( do
              resp <- readTMVar resultVar
              mbox' <- readTVar mbox
              writeTVar mbox (Map.delete reqId mbox')
              pure resp
          )
    )
    >>= \case
      MkResponse _ _ (Failure errmsg) -> pure $ Left errmsg
      MkResponse _ (Just actualLeaderId) NotLeader -> request actualLeaderId entry
      MkResponse _ (Just leader) (Success result) -> pure $ Right (leader, result)
      -- TODO: what if leader is @Nothing@ but response is `Success`??
      MkResponse _ Nothing _ -> pure $ Left "No known leaders"
