module Control.Distributed.Process.Raft.Admin
  ( withRaftAdmin,

    -- * Re-exports
    joinCluster,
    leaveCluster,
    getClusterConfiguration,
    shutDown,
  )
where

import Control.Concurrent.STM (atomically, newTQueueIO, readTQueue, writeTQueue)
import Control.Distributed.Process (NodeId, Process, exit, getSelfNode, link, match, matchUnknown, nsendRemote, receiveWait, register, spawnLocal)
import Control.Distributed.Process.Raft.Instances ()
import Control.Monad (forever)
import Control.Monad.IO.Class (liftIO)
import Distributed.Consensus.Raft (ClusterConfiguration)
import Distributed.Consensus.Raft.Admin (AdminError, AdminImplementation (..), RaftAdminT, withRaftAdminT)
import Distributed.Consensus.Raft.Admin qualified as Raft.Admin

-- | Send an admin request to a Raft cluster.
--
-- It is perfectly safe, and encouraged, to send separate requests in
-- separate threads.
withRaftAdmin ::
  ((forall a. RaftAdminT NodeId Process a -> Process a) -> Process b) ->
  Process b
withRaftAdmin f = do
  recvQueue <- liftIO newTQueueIO

  mailPid <-
    spawnLocal $
      forever $
        receiveWait
          [ match $ liftIO . atomically . writeTQueue recvQueue,
            matchUnknown $ pure ()
          ]

  link mailPid

  register adminMailboxProcessName mailPid

  thisNodeId <- getSelfNode

  withRaftAdminT thisNodeId (impl recvQueue) f <* exit mailPid "Completed"
  where
    impl recvQueue =
      AdminImplementation
        { sendAdminRequest = (`nsendRemote` adminMailboxProcessName),
          receiveAdminResponse = liftIO $ atomically $ readTQueue recvQueue
        }

-- | This value needs to be kept in sync with 'Control.Distributed.Process.Raft'.
-- The alternative would be a tiny shared library...
adminMailboxProcessName :: String
adminMailboxProcessName = "raft-consensus-admin-mailbox"

joinCluster ::
  -- | Node to command
  NodeId ->
  -- \| Node to join
  NodeId ->
  RaftAdminT NodeId Process (Either (AdminError NodeId) ())
joinCluster = Raft.Admin.joinCluster

leaveCluster ::
  -- | Node to command
  NodeId ->
  RaftAdminT NodeId Process (Either (AdminError NodeId) ())
leaveCluster = Raft.Admin.leaveCluster

getClusterConfiguration ::
  -- | Node to ask.
  NodeId ->
  RaftAdminT NodeId Process (Either (AdminError NodeId) (ClusterConfiguration NodeId))
getClusterConfiguration = Raft.Admin.getClusterConfiguration

shutDown ::
  -- | Node to command
  NodeId ->
  RaftAdminT NodeId Process (Either (AdminError NodeId) ())
shutDown = Raft.Admin.shutDown
