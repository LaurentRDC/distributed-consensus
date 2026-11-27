-- |
-- Module:      $module
-- Copyright:   (c) Laurent P. René de Cotret
-- License:     BSD 3-Clause
--
-- Implementation of the <https://raft.github.io/raft.pdf Raft distributed consensus algorithm>,
-- generic over networking and persistence.
--
-- === Why make use of distributed consensus
--
-- Distributed consensus allows to replicate a state machine across separate nodes. The major advantage
-- is that a failure in one of the nodes does not lead to data loss, as the state machine's state is
-- replicated on all nodes.
--
-- In the jargon, distributed consensus gives you a state machine which is strongly-consistent and fault-tolerant.
--
-- Applications of distributed consensus include boring but important things like distributed key-value store,
-- or exciting things like distributed software transactional memory.
--
-- In practice, you should avoid having to use distributed consensus as much as possible. Strong consistency
-- is desirable, but it comes at the cost of performance. It is also phenomenally difficult to implement correctly
-- (more on this below).
-- Nonetheless, if you persist in your desire to use distributed consensus, this library aims to be both
-- correct and performant.
--
-- === Why use the @raft-consensus@ library
--
-- This library implements the core of the Raft algorithm while being agnostic to the way you implement
-- networking and persistence. It is designed to be versatile and adapt to your needs.
-- The genericity also serves to improve the testing story.
--
-- === How is @raft-consensus@ tested?
--
-- @raft-consensus@ makes use of a testing technique known as deterministic simulation testing,
-- which finds bugs in the actual implementation by simulating operating conditions without actually doing any IO.
-- The test suite uses deterministic simulation testing to execute the cluster logic over a wide range of
-- inputs, and injects deterministic faults such as crashes and network partitions.
--
-- Cluster properties (such as the expectation that a cluster leader has the
-- correct state) are continuously monitored to ensure that the implementation of @raft-consensus@ behaves
-- in practice like the algorithm is designed.
--
-- In parallel, client requests are monitored during simulations, and their linearizability is checked
-- using the linearizability checker from the @linearizable@ package.
--
-- === How does one use @raft-consensus@?
--
-- @raft-consensus@ requires a few things from you:
--
-- 1. You need to define your state machine of type 'StateMachineStep'.
-- 2. You need to implement networking by creating a record of functions of type 'Networking'.
--    A separate package @raft-consensus-distributed-process@ is provided in case you wish
--    to use Cloud Haskell (@distributed-process@) for networking.
-- 3. You need to implement persistence by creating a record of functions of type 'Persistence'.
--    A separate package @raft-consensus-wal@ is provided in case you wish to use
--    a write-ahead log from the @wal@ package.
--
-- Once this is done, you are ready to start individual nodes of your cluster using 'runRaftServer'
-- or 'withRaftServer'.
--
-- === How does one interact with a @raft-consensus@-based cluster?
--
-- There are two ways to interact with a cluster:
--
-- * The typical way is as a client. A client is a process that sends requests to either read
--   from the state machine, or mutate the state machine. This functionality is provided in
--   the "Distributed.Consensus.Raft.Client" module.
-- * The other way is as a cluster administrator. An administrator is a process that sends
--   administrative requests, including requesting that a node join or leave a cluster.
--   This functionality is provided in the "Distributed.Consensus.Raft.Admin" module.
--
-- === Examples
--
-- The <https://github.com/LaurentRDC/distributed-consensus source repository> contains examples
-- of applications that make use of @raft-consensus@ and associated libraries.
module Distributed.Consensus.Raft
  ( -- * Server
    runRaftServer,
    withRaftServer,

    -- ** Configuration
    simpleConfig,
    Config (..),

    -- ** Configuration types
    Microseconds,
    ClusterState (..),

    -- * Protocol implementation
    Implementation (..),
    Networking (..),
    Persistence (..),
    StateMachineStep (..),
    runStateMachineStep,

    -- ** Helpers
    nullTracer,
    ephemeralPersistence,

    -- ** Domain types
    LogEntry (..),
    Term,
    Role,
    ClientRequestId,
    ClusterConfiguration (..),
    LogIndex,
    Snapshot (..),
    SnapshotMetadata (..),
    SessionId,

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
    SessionId,
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
    ephemeralPersistence,
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

-- | Run a Raft server. This function returns in case of an administrative
-- request to shut down.
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
