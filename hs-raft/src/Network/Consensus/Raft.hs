module Network.Consensus.Raft
  ( -- * Server
    runRaftServer,
    Config (..),

    -- ** Configuration types
    Microseconds (Microseconds),
    ClusterState (..),

    -- * Protocol specification
    Specification (..),

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
import Network.Consensus.Raft.Algorithm (server)
import Network.Consensus.Raft.Domain
  ( ClusterConfiguration (..),
    LogIndex,
    RequestId,
    Role,
    Snapshot (..),
    SnapshotMetadata (..),
    Term,
  )
import Network.Consensus.Raft.Log (Log)
import Network.Consensus.Raft.Messaging
  ( Request (..),
    Response (..),
  )
import Network.Consensus.Raft.Timer (Microseconds (..))
import Network.Consensus.Raft.Transformer
  ( ClusterState (..),
    Config (..),
    runRaftT,
  )
import Network.Consensus.Raft.Transformer.Spec
  ( AppendEntries (..),
    AppendEntriesResult (..),
    ClusterMembershipError (..),
    ClusterMembershipRequest (..),
    ClusterMembershipResult (..),
    Command (..),
    CommandResponse (..),
    Event (..),
    EventContext (..),
    InstallSnapshot (..),
    InstallSnapshotResult (..),
    LogEntry (..),
    RPC (..),
    RPCResult (..),
    RaftTrace (..),
    Specification (..),
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
  Specification entry node state result m ->
  m ()
runRaftServer config startingState initState spec =
  runRaftT config startingState initState spec server
