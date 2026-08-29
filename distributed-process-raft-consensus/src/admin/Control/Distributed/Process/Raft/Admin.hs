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
import Control.Distributed.Process (Process, ProcessId, exit, link, match, matchUnknown, receiveWait, send, spawnLocal)
import Control.Distributed.Process.Raft.Instances ()
import Control.Monad (forever)
import Control.Monad.IO.Class (liftIO)
import Distributed.Consensus.Raft (ClusterConfiguration)
import Distributed.Consensus.Raft.Admin (AdminError, AdminImplementation (..), RaftAdminT, withRaftAdminT)
import Distributed.Consensus.Raft.Admin qualified as Raft.Admin

-- | Send a request to a Raft cluster.
--
-- It is perfectly safe, and encouraged, to send separate requests in
-- separate threads.
--
-- See 'request' to build an appropriate request.
withRaftAdmin ::
  ((forall a. RaftAdminT ProcessId Process a -> Process a) -> Process b) ->
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

  -- Note that we want our self-identification to point
  -- not this THIS process ID, but the process ID
  -- awaiting messages.
  withRaftAdminT mailPid (impl recvQueue) f <* exit mailPid "Completed"
  where
    impl recvQueue =
      AdminImplementation
        { sendAdminRequest = send,
          receiveAdminResponse = liftIO $ atomically $ readTQueue recvQueue
        }

joinCluster ::
  -- | Node to command
  ProcessId ->
  -- \| Node to join
  ProcessId ->
  RaftAdminT ProcessId Process (Either (AdminError ProcessId) ())
joinCluster = Raft.Admin.joinCluster

leaveCluster ::
  -- | Node to command
  ProcessId ->
  RaftAdminT ProcessId Process (Either (AdminError ProcessId) ())
leaveCluster = Raft.Admin.leaveCluster

getClusterConfiguration ::
  -- | Node to ask.
  ProcessId ->
  RaftAdminT ProcessId Process (Either (AdminError ProcessId) (ClusterConfiguration ProcessId))
getClusterConfiguration = Raft.Admin.getClusterConfiguration

shutDown ::
  -- | Node to command
  ProcessId ->
  RaftAdminT ProcessId Process (Either (AdminError ProcessId) ())
shutDown = Raft.Admin.shutDown
