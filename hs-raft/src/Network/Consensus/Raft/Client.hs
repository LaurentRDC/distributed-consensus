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
    requestMany,

    -- * Communications between clients and clusters
    Request (..),
    Response (..),
  )
where

import Control.Arrow ((&&&))
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Reader (ReaderT (runReaderT), asks)
import Data.Bifunctor (second)
import Data.Binary (Binary)
import Data.List.NonEmpty (NonEmpty)
import qualified Data.List.NonEmpty as NonEmpty
import Data.Text (Text)
import GHC.Generics (Generic)
import Lens.Micro.Platform (makeLenses)

-- Alphabet for communicating with clients

data Request node entry
  = MkRequest
  { requestOriginator :: !node,
    requestEntries :: !(NonEmpty entry)
  }
  deriving (Eq, Show, Generic)

instance (Binary node, Binary entry) => Binary (Request node entry)

data Response node result
  = Success !node !(NonEmpty result)
  | Failure !Text
  | NotLeader (Maybe node)
  deriving (Eq, Show, Generic)

instance (Binary node, Binary result) => Binary (Response node result)

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
--
-- If you want to send multiple entries in a single call, see 'requestMany'.
request ::
  (Monad m) =>
  node ->
  entry ->
  RaftClientT entry node result m (Either Text result)
request lastKnownLeader =
  fmap (second NonEmpty.head)
    . requestMany lastKnownLeader
    . NonEmpty.singleton

-- | Send a request to a Raft cluster including multiple entries.
requestMany ::
  (Monad m) =>
  node ->
  NonEmpty entry ->
  RaftClientT entry node result m (Either Text (NonEmpty result))
requestMany lastKnownLeader entries = do
  (self, (send, recv)) <- MkRaftClientT $ asks (node &&& (sendRequest &&& receiveResponse) . specification)
  resp <- MkRaftClientT $ lift (send lastKnownLeader (MkRequest self entries) >> recv)
  case resp of
    Left errmsg -> pure $ Left errmsg
    Right (Failure errmsg) -> pure $ Left errmsg
    Right (NotLeader (Just actualLeaderId)) -> requestMany actualLeaderId entries
    Right (NotLeader Nothing) -> pure $ Left "No known leaders"
    Right (Success _ result) -> pure $ Right result
