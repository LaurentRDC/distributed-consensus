module Control.Distributed.Process.Raft.Client
  ( withRaftClient,
    request,
  )
where

import Control.Concurrent.STM (atomically, newTQueueIO, readTQueue, writeTQueue)
import Control.Distributed.Process (Process, ProcessId, exit, link, match, matchUnknown, receiveWait, send, spawnLocal)
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
  ((forall a. RaftClientT entry ProcessId result Process a -> Process a) -> Process b) ->
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

  -- Note that we want our self-identification to point
  -- not this THIS process ID, but the process ID
  -- awaiting messages.
  withRaftClientT mailPid (impl recvQueue) f <* exit mailPid "Completed"
  where
    impl recvQueue =
      ClientImplementation
        { sendRequest = send,
          receiveResponse = liftIO $ atomically $ readTQueue recvQueue
        }

request ::
  ProcessId ->
  entry ->
  RaftClientT entry ProcessId result Process (Either Text (ProcessId, result))
request = Raft.Client.request
