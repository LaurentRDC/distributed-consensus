module Control.Distributed.Process.Raft.Admin
  ( withRaftAdmin,

    -- * Re-exports
    AdminError (..),
    Microseconds,
    sendAdminCommand,
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
import Distributed.Consensus.Raft.Admin (AdminCommandResult, AdminError, AdminImplementation (..), AdminRequest, Microseconds, RaftAdminT, sessionId, withRaftAdminT)
import Distributed.Consensus.Raft.Admin qualified as Raft.Admin
import System.Random (randomIO)

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
          receiveAdminResponse = liftIO $ atomically $ readTQueue recvQueue,
          generateSessionId =
            const $
              liftIO $
                sessionId <$> randomIO <*> randomIO
        }

-- | This value needs to be kept in sync with 'Control.Distributed.Process.Raft'.
-- The alternative would be a tiny shared library...
adminMailboxProcessName :: String
adminMailboxProcessName = "raft-consensus-admin-mailbox"

sendAdminCommand ::
  NodeId ->
  AdminRequest NodeId ->
  RaftAdminT
    NodeId
    Process
    ( Either
        (AdminError NodeId)
        (AdminCommandResult NodeId)
    )
sendAdminCommand = Raft.Admin.sendAdminCommand

joinCluster ::
  -- \| Node to join
  NodeId ->
  RaftAdminT NodeId Process (AdminRequest NodeId)
joinCluster = Raft.Admin.joinCluster

leaveCluster ::
  RaftAdminT NodeId Process (AdminRequest NodeId)
leaveCluster = Raft.Admin.leaveCluster

getClusterConfiguration ::
  RaftAdminT NodeId Process (AdminRequest NodeId)
getClusterConfiguration = Raft.Admin.getClusterConfiguration

shutDown ::
  RaftAdminT NodeId Process (AdminRequest NodeId)
shutDown = Raft.Admin.shutDown
