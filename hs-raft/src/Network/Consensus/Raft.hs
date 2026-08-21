module Network.Consensus.Raft
  ( -- TODO: trim exports to minimum interface
    module Network.Consensus.Raft.Domain,
    module Network.Consensus.Raft.Messaging,
    module Network.Consensus.Raft.Transformer,
    Microseconds (Microseconds),
    Log,

    -- * Run server
    runRaftServer,
  )
where

import Control.Concurrent.Class.MonadMVar (MonadMVar)
import Control.Monad.Class.MonadAsync (MonadAsync)
import Control.Monad.Class.MonadFork (MonadFork)
import Control.Monad.Class.MonadThrow (MonadMask)
import Control.Monad.Class.MonadTimer (MonadDelay)
import Network.Consensus.Raft.Algorithm (server)
import Network.Consensus.Raft.Domain
import Network.Consensus.Raft.Log (Log)
import Network.Consensus.Raft.Messaging
import Network.Consensus.Raft.Timer (Microseconds (..))
import Network.Consensus.Raft.Transformer

runRaftServer ::
  ( Ord node,
    MonadMVar m,
    MonadMask m,
    MonadDelay m,
    MonadAsync m,
    MonadFork m
  ) =>
  Config node ->
  ClusterState node ->
  state ->
  RaftSpec entry node state result m ->
  m ()
runRaftServer config startingState initState spec =
  runRaftT config startingState initState spec server
