module Distributed.Consensus.Raft
  ( -- * Server
    runRaftServer,
    withRaftServer,

    -- ** Configuration
    simpleConfig,
    Config (..),

    -- ** Configuration types
    Microseconds (Microseconds),
    ClusterState (..),

    -- * Protocol implementation
    Implementation (..),
    Networking (..),
    Persistence (..),
    StateMachineStep (..),
    runStateMachineStep,

    -- ** Helpers
    nullTracer,

    -- ** Domain types
    LogEntry (..),
    Term,
    Role,
    ClientRequestId,
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
    Log,
  )
where

import Control.Concurrent.Class.MonadMVar (MonadMVar)
import Control.Monad.Class.MonadAsync (MonadAsync, link, withAsync)
import Control.Monad.Class.MonadFork (MonadFork)
import Control.Monad.Class.MonadThrow (MonadMask)
import Control.Monad.Class.MonadTimer (MonadDelay)
import Distributed.Consensus.Raft.Algorithm (server)
import Distributed.Consensus.Raft.Client (ClientRequestId)
import Distributed.Consensus.Raft.Domain
  ( ClusterConfiguration (..),
    LogIndex,
    Role,
    Snapshot (..),
    SnapshotMetadata (..),
    StateMachineStep (..),
    Term,
    runStateMachineStep,
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
    Networking (..),
    Persistence (..),
    RPC (..),
    RPCResult (..),
    RaftTrace (..),
    nullTracer,
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
    simpleConfig,
  )

-- | Run a Raft server. This function never returns.
--
-- See also 'withRaftServer'.
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
  Implementation entry node result state m ->
  m ()
runRaftServer config startingState initState impl =
  runRaftT config startingState initState impl server

-- | Run a Raft server in a separate thread, while running a separate action.
--
-- Once the action returns, the server is shut down.
withRaftServer ::
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
  Implementation entry node result state m ->
  m a ->
  m a
withRaftServer config startingState initState impl f =
  withAsync (runRaftServer config startingState initState impl) $ \serverThread -> do
    link serverThread
    f
