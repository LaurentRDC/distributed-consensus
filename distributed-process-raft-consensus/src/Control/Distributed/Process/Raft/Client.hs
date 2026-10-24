module Control.Distributed.Process.Raft.Client
  ( withRaftClient,
    request,
    Microseconds,
    ClientError (..),
  )
where

import Control.Concurrent.STM (atomically, newTQueueIO, readTQueue, writeTQueue)
import Control.Distributed.Process (NodeId, Process, exit, getSelfNode, link, match, matchUnknown, nsendRemote, receiveWait, register, spawnLocal)
import Control.Distributed.Process.Raft.Instances ()
import Control.Distributed.Process.Serializable (Serializable)
import Control.Monad (forever)
import Control.Monad.IO.Class (liftIO)
import Distributed.Consensus.Raft.Client (ClientError, ClientImplementation (..), Microseconds, RaftClientT, withRaftClientT)
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

-- | Send a request to a Raft cluster.
--
-- The user must take care to handle the possible 'ClientError's.
--
-- First, it is quite likely that the server will redirect requests
-- via @'Redirect' otherServer@. 'request' does not follow that redirect,
-- as you may want to control delays and retries.
--
-- Second, there is an unlikely, but real, possibility that the server
-- replies with 'NoKnownLeader'. In this case, you should try two things:
--
-- * Retry the request, which may resolve itself
-- * If retrying doesn't work, you should try to send the request to another server.
--   It may be beneficial to keep a memory of all servers ever contacted for this
--   particular purpose.
--
-- It is perfectly safe, and encouraged, to send separate requests in
-- separate threads.
request ::
  NodeId ->
  entry ->
  RaftClientT
    entry
    NodeId
    result
    Process
    (Either (ClientError NodeId) (NodeId, result))
request = Raft.Client.request
