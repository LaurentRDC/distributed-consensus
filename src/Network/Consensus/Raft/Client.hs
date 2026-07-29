{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}

module Network.Consensus.Raft.Client
  ( RaftClientT,
    RaftClientSpec (..),
    runRaftClientT,
    request,

    -- * Communications between clients and clusters
    Request (..),
    Response (..),
  )
where

import Control.Arrow ((&&&))
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Reader (ReaderT (runReaderT), asks)
import Data.Text (Text)
import Lens.Micro.Platform (makeLenses)

-- Alphabet for communicating with clients

data Request node entry
  = MkRequest
  { requestOriginator :: !node,
    requestEntry :: !entry
  }
  deriving (Eq, Show)

data Response node result
  = Success !node !result
  | Failure !Text
  | NotLeader (Maybe node)
  deriving (Eq, Show)

newtype RaftClientT entry node result m a
  = MkRaftClientT (ReaderT (RaftClientEnv entry node result m) m a)
  deriving (Functor, Applicative, Monad)

data RaftClientSpec entry node result m = MkRaftClientSpec
  { sendRequest :: node -> Request node entry -> m (),
    receiveResponse :: m (Either Text (Response node result))
  }

runRaftClientT ::
  RaftClientT entry node result m a ->
  -- | self identification
  node ->
  RaftClientSpec entry node result m ->
  m a
runRaftClientT (MkRaftClientT f) self spec = runReaderT f (MkRaftClientEnv self spec)

data RaftClientEnv entry node result m
  = MkRaftClientEnv
  { node :: !node,
    specification :: RaftClientSpec entry node result m
  }

makeLenses ''RaftClientSpec

-- | Send a request to a Raft cluster.
request ::
  (Monad m) =>
  node ->
  entry ->
  RaftClientT entry node result m (Either Text result)
request lastKnownLeader entry = do
  (self, (send, recv)) <- MkRaftClientT $ asks (node &&& (sendRequest &&& receiveResponse) . specification)
  resp <- MkRaftClientT $ lift (send lastKnownLeader (MkRequest self entry) >> recv)
  case resp of
    Left errmsg -> pure $ Left errmsg
    Right (Failure errmsg) -> pure $ Left errmsg
    Right (NotLeader (Just actualLeaderId)) -> request actualLeaderId entry
    Right (NotLeader Nothing) -> pure $ Left "No known leaders"
    Right (Success _ result) -> pure $ Right result
