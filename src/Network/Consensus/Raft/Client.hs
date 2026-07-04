{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE TemplateHaskell #-}

module Network.Consensus.Raft.Client
  ( RaftClientT,
    RaftClientSpec (..),
    runRaftClientT,
    request,
  )
where

import Control.Arrow ((&&&))
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Reader (ReaderT (runReaderT), asks)
import Lens.Micro.Platform (makeLenses)

newtype RaftClientT entry result m a
  = MkRaftClientT (ReaderT (RaftClientEnv entry result m) m a)
  deriving (Functor, Applicative, Monad)

data RaftClientSpec entry result m = MkRaftClientSpec
  { sendRequest :: entry -> m (),
    receiveResponse :: m (Maybe result)
  }

runRaftClientT ::
  RaftClientT entry result m a ->
  RaftClientSpec entry result m ->
  m a
runRaftClientT (MkRaftClientT f) spec = runReaderT f (MkRaftClientEnv spec)

newtype RaftClientEnv entry result m
  = MkRaftClientEnv
  { -- TODO: keep cache of current leader
    specification :: RaftClientSpec entry result m
  }

makeLenses ''RaftClientSpec

request :: (Monad m) => entry -> RaftClientT entry result m (Maybe result)
request entry = MkRaftClientT $ do
  (send, recv) <- asks ((sendRequest &&& receiveResponse) . specification)
  lift $ do
    send entry
    recv
