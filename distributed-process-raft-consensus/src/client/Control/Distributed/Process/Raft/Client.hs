module Control.Distributed.Process.Raft.Client
  ( withRaftClient,
    request,
  )
where

import Control.Concurrent.STM (atomically, newTQueueIO, readTQueue, writeTQueue)
import Control.Distributed.Process (NodeId, Process, exit, getSelfNode, link, match, matchUnknown, nsendRemote, receiveWait, register, spawnLocal)
import Control.Distributed.Process.Raft.Instances ()
import Control.Distributed.Process.Serializable (Serializable)
import Control.Monad (forever)
import Control.Monad.IO.Class (liftIO)
import Data.Text (Text)
import Distributed.Consensus.Raft.Client (ClientImplementation (..), RaftClientT, withRaftClientT)
import Distributed.Consensus.Raft.Client qualified as Raft.Client

-- | Send a request to a Raft cluster.
--
-- It is perfectly safe, and encouraged, to send separate requests in
-- separate threads.
--
-- See 'request' to build an appropriate request.
withRaftClient ::
  (Serializable entry, Serializable result) =>
  ((forall a. RaftClientT entry NodeId result Process a -> Process a) -> Process b) ->
  Process b
withRaftClient f = do
  recvQueue <- liftIO newTQueueIO

  mailPid <-
    spawnLocal $
      forever $
        receiveWait
          [ match $ liftIO . atomically . writeTQueue recvQueue,
            matchUnknown $ pure ()
          ]

  link mailPid

  register clientMailboxProcessName mailPid

  thisNodeId <- getSelfNode

  withRaftClientT thisNodeId (impl recvQueue) f <* exit mailPid "Completed"
  where
    impl recvQueue =
      ClientImplementation
        { sendRequest = (`nsendRemote` clientMailboxProcessName),
          receiveResponse = liftIO $ atomically $ readTQueue recvQueue
        }

-- | This value needs to be kept in sync with 'Control.Distributed.Process.Raft'.
-- The alternative would be a tiny shared library...
clientMailboxProcessName :: String
clientMailboxProcessName = "raft-consensus-client-mailbox"

request ::
  NodeId ->
  entry ->
  RaftClientT entry NodeId result Process (Either Text (NodeId, result))
request = Raft.Client.request
