module Distributed.Consensus.Raft
  ( -- * Server
    runRaftServer,
    Config (..),

    -- ** Configuration types
    Microseconds (Microseconds),
    ClusterState (..),

    -- * Protocol implementation
    Implementation (..),

    -- ** Domain types
    LogEntry (..),
    Term,
    Role,
    ClusterConfiguration (..),
    LogIndex,
    Snapshot (..),
    SnapshotMetadata (..),

    -- ** Remote procedure calls
    RPC (..),
    RPCResult (..),
    Command (..),
    CommandResponse (..),
    AppendEntries (..),
    AppendEntriesResult (..),
    InstallSnapshot (..),
    InstallSnapshotResult (..),
    ClusterMembershipRequest (..),
    ClusterMembershipResult (..),
    ClusterMembershipError (..),

    -- ** Messaging
    Request (..),
    Response (..),

    -- ** Event tracing
    Event (..),
    EventContext (..),
    RaftTrace (..),
    RequestId,
    Log,
  )
where

import Control.Concurrent.Class.MonadMVar (MonadMVar)
import Control.Monad.Class.MonadAsync (MonadAsync)
import Control.Monad.Class.MonadFork (MonadFork)
import Control.Monad.Class.MonadThrow (MonadMask)
import Control.Monad.Class.MonadTimer (MonadDelay)
import Distributed.Consensus.Raft.Algorithm (server)
import Distributed.Consensus.Raft.Domain
  ( ClusterConfiguration (..),
    LogIndex,
    RequestId,
    Role,
    Snapshot (..),
    SnapshotMetadata (..),
    Term,
  )
import Distributed.Consensus.Raft.Implementation
  ( AppendEntries (..),
    AppendEntriesResult (..),
    ClusterMembershipError (..),
    ClusterMembershipRequest (..),
    ClusterMembershipResult (..),
    Command (..),
    CommandResponse (..),
    Event (..),
    EventContext (..),
    Implementation (..),
    InstallSnapshot (..),
    InstallSnapshotResult (..),
    LogEntry (..),
    RPC (..),
    RPCResult (..),
    RaftTrace (..),
  )
import Distributed.Consensus.Raft.Log (Log)
import Distributed.Consensus.Raft.Messaging
  ( Request (..),
    Response (..),
  )
import Distributed.Consensus.Raft.Timer (Microseconds (..))
import Distributed.Consensus.Raft.Transformer
  ( ClusterState (..),
    Config (..),
    runRaftT,
  )

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
  Implementation entry node state result m ->
  m ()
runRaftServer config startingState initState impl =
  runRaftT config startingState initState impl server
