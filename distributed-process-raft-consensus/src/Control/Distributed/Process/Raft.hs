{-# LANGUAGE LambdaCase #-}

module Control.Distributed.Process.Raft
  ( networking,
  )
where

import Control.Concurrent.STM (atomically, flushTQueue, newTQueueIO, readTQueue, retry, writeTQueue)
import Control.Distributed.Process (NodeId, Process, link, match, matchUnknown, nsendRemote, receiveWait, register, spawnLocal)
-- Simply importing the instances here is enough
-- for someone else importing Control.Distributed.Process.Raft
-- to get access to the instances
import Control.Distributed.Process.Raft.Instances ()
import Control.Distributed.Process.Serializable (Serializable)
import Control.Monad (forever)
import Control.Monad.IO.Class (liftIO)
import Data.List.NonEmpty (NonEmpty (..))
import Distributed.Consensus.Raft (Networking (..))

networking ::
  (Serializable entry, Serializable state, Serializable result) =>
  Process (Networking entry NodeId state result Process)
networking = do
  (rpcQueue, rpcResultQueue, clientRequestQueue, adminRequestQueue) <-
    liftIO $
      (,,,)
        <$> newTQueueIO
        <*> newTQueueIO
        <*> newTQueueIO
        <*> newTQueueIO

  -- Registering the mailbox process under a name
  -- is what allows other nodes to send messages to this node
  -- knowing only a `NodeId` (essentially host:port)
  --
  -- It is important to keep the process name stable, as clients and admins
  -- need to receive/send based on these process names as well
  --
  -- TODO: it's not clear if 'nsendRemote' is measurably slower than 'send',
  --       If it is, we could use a IORef to cache a 'Map NodeId ProcessId'.
  --       Then, on the first 'sendX', we first query the node to ask which
  --       'ProcessId' is the mailbox one
  spawnMailbox rpcMailboxProcessName (recvLoop rpcQueue rpcResultQueue)
  spawnMailbox clientMailboxProcessName (clientRecvLoop clientRequestQueue)
  spawnMailbox adminMailboxProcessName (adminRecvLoop adminRequestQueue)

  pure
    Networking
      { sendRPC = (`nsendRemote` rpcMailboxProcessName),
        sendRPCResult = (`nsendRemote` rpcMailboxProcessName),
        sendClientResponse = (`nsendRemote` rpcMailboxProcessName),
        sendAdminResponse = (`nsendRemote` rpcMailboxProcessName),
        receiveRPC = liftIO $ atomically $ readTQueue rpcQueue,
        receiveRPCResult = liftIO $ atomically $ readTQueue rpcResultQueue,
        receiveAdminRequest = liftIO $ atomically $ readTQueue adminRequestQueue,
        receiveClientRequests = liftIO $ atomically $ do
          -- Flushing the queue and retrying on empty
          -- allows pipelining client requests
          flushTQueue clientRequestQueue >>= \case
            [] -> retry
            (r : rs) -> pure $ r :| rs
      }
  where
    spawnMailbox :: String -> Process () -> Process ()
    spawnMailbox processName mailbox = do
      pid <- spawnLocal mailbox
      link pid
      register processName pid

    rpcMailboxProcessName = "raft-consensus-mailbox"
    clientMailboxProcessName = "raft-consensus-client-mailbox"
    adminMailboxProcessName = "raft-consensus-admin-mailbox"

    recvLoop rpcQueue rpcResultQueue =
      forever $
        receiveWait
          [ match $ liftIO . atomically . writeTQueue rpcQueue,
            match $ liftIO . atomically . writeTQueue rpcResultQueue,
            -- flush the queue from unknown message types.
            matchUnknown (pure ())
          ]
    clientRecvLoop clientRequestQueue =
      forever $
        receiveWait
          [ match $ liftIO . atomically . writeTQueue clientRequestQueue,
            -- flush the queue from unknown message types.
            matchUnknown (pure ())
          ]

    adminRecvLoop adminRequestQueue =
      forever $
        receiveWait
          [ match $ liftIO . atomically . writeTQueue adminRequestQueue,
            -- flush the queue from unknown message types.
            matchUnknown (pure ())
          ]
