{-# LANGUAGE LambdaCase #-}

module Control.Distributed.Process.Raft
  ( networking,
  )
where

import Control.Concurrent.STM (atomically, flushTQueue, newTQueueIO, readTQueue, retry, writeTQueue)
import Control.Distributed.Process (Process, ProcessId, link, match, matchUnknown, receiveWait, send, spawnLocal)
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
  Process
    ( -- \| 'ProcessId' where messages should be sent.
      -- This is the value you should pass to 'Config'

      ProcessId,
      Networking
        entry
        ProcessId
        state
        result
        Process
    )
networking = do
  (rpcQueue, rpcResultQueue, clientRequestQueue, adminRequestQueue) <-
    liftIO $
      (,,,)
        <$> newTQueueIO
        <*> newTQueueIO
        <*> newTQueueIO
        <*> newTQueueIO

  -- All incoming messages get process by the same
  -- loop, because that way the server has a single entry point.
  --
  -- Messages are de-multiplexed to transactional queues to
  -- match what `raft-consensus` expects.
  mailPid <-
    spawnLocal
      ( recvLoop
          rpcQueue
          rpcResultQueue
          clientRequestQueue
          adminRequestQueue
      )

  link mailPid

  pure
    ( mailPid,
      Networking
        { sendRPC = send,
          sendRPCResult = send,
          sendClientResponse = send,
          sendAdminResponse = send,
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
    )
  where
    recvLoop rpcQueue rpcResultQueue clientRequestQueue adminRequestQueue =
      forever $
        receiveWait
          [ match $ liftIO . atomically . writeTQueue rpcQueue,
            match $ liftIO . atomically . writeTQueue rpcResultQueue,
            match $ liftIO . atomically . writeTQueue clientRequestQueue,
            match $ liftIO . atomically . writeTQueue adminRequestQueue,
            -- flush the queue from unknown message types.
            matchUnknown (pure ())
          ]
