{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

module Test.Control.Distributed.Process.Raft (tests) where

import Control.Concurrent.Async (withAsync)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, readMVar, takeMVar)
import Control.Concurrent.STM (atomically, newTVarIO, readTVar, retry, writeTVar)
import Control.Distributed.Process (NodeId (NodeId), spawnLocal)
import Control.Distributed.Process.Node (LocalNode (localNodeId), initRemoteTable, newLocalNode, runProcess)
import Control.Distributed.Process.Raft (networking)
import Control.Distributed.Process.Raft.Admin (getClusterConfiguration, joinCluster, leaveCluster, shutDown, withRaftAdmin)
import Control.Distributed.Process.Raft.Client (request, withRaftClient)
import Control.Monad (replicateM_, unless, void)
import Control.Monad.IO.Class (liftIO)
import Data.Foldable (for_)
import Data.List.NonEmpty (NonEmpty ((:|)))
import Distributed.Consensus.Raft (Networking (..), Request (..), Response (..), requestPayload)
import Distributed.Consensus.Raft.Admin (AdminCommand (..), AdminCommandResult (..))
import Network.Transport (EndPointAddress (EndPointAddress), Transport)
import qualified Network.Transport.InMemory as InMemory
import System.Timeout (timeout)
import Test.Tasty (TestTree, testGroup, withResource)
import Test.Tasty.HUnit (assertFailure, testCase, (@?=))

tests :: TestTree
tests = withResource InMemory.createTransport (\_ -> pure ()) $
  \getTransport ->
    testGroup
      "Control.Distributed.Process.Raft"
      [ testAdminCommands getTransport,
        testClientCommands getTransport
      ]

testAdminCommands :: IO Transport -> TestTree
testAdminCommands getTransport = testCase "Tests that admin commands are communicated" $ do
  t <- getTransport
  node <- newLocalNode t initRemoteTable
  ready <- newEmptyMVar
  let serverNodeId = localNodeId node
  withAsync (sendAdminCommands ready t serverNodeId) $ \_ -> runProcess node $ do
    serverNetwork <- networking @() @() @()
    liftIO $ putMVar ready ()
    let expectations =
          [ (JoinCluster arbitraryOtherNodeId, JoinInitiated),
            (LeaveCluster, LeaveInitiated),
            (GetClusterConfiguration, AdminFailure "testing"),
            (ShutDown, ShutdownInitiated)
          ]
    for_ expectations $ \(expectation, response) -> do
      req <- receiveAdminRequest serverNetwork
      liftIO (requestPayload req @?= expectation)
      sendAdminResponse serverNetwork (requestOriginator req) (MkResponse (requestId req) Nothing response)
  where
    arbitraryOtherNodeId :: NodeId
    arbitraryOtherNodeId = NodeId $ EndPointAddress "some-arbitrary-node-id"

    sendAdminCommands ready transport serverNodeId = do
      _ <- liftIO $ takeMVar ready
      node <- newLocalNode transport initRemoteTable
      runProcess node $ withRaftAdmin $ \runAdmin -> runAdmin $ do
        -- Since the helper functions e.g. 'joinCluster' automatically wait for a response,
        -- we spin up a new thread for each of them
        void $ joinCluster 1_000_000 serverNodeId arbitraryOtherNodeId
        void $ leaveCluster 1_000_000 serverNodeId
        void $ getClusterConfiguration 1_000_000 serverNodeId
        void $ shutDown 1_000_000 serverNodeId

testClientCommands :: IO Transport -> TestTree
testClientCommands getTransport = testCase "Tests that client commands are communicated" $ do
  t <- getTransport
  node <- newLocalNode t initRemoteTable
  ready <- newEmptyMVar
  requestsDone <- newTVarIO (0 :: Int)
  let serverNodeId = localNodeId node
  requestsVar <- newEmptyMVar
  withAsync (sendClientCommands ready requestsDone t serverNodeId) $ \_ -> runProcess node $ do
    serverNetwork <- networking @() @() @()
    liftIO $ putMVar ready ()

    -- We wait for all client requests to be completed
    -- to prevent the test from relying on timing
    liftIO $ atomically $ do
      n <- readTVar requestsDone
      unless (n == nRequests) retry

    receiveClientRequests serverNetwork
      >>= liftIO . putMVar requestsVar

  timeout
    1_000_000
    (readMVar requestsVar)
    >>= \case
      Nothing -> assertFailure "Server did not receive expected number of requests"
      Just requests -> do
        let strippedRequests = requestPayload <$> requests
        strippedRequests @?= () :| [(), ()]
  where
    nRequests = 3
    sendClientCommands ready requestsDone transport serverNodeId = do
      _ <- liftIO $ takeMVar ready
      node <- newLocalNode transport initRemoteTable
      runProcess node $ withRaftClient @() @() $ \runClient -> do
        -- To demonstrate that pipelining works, we send multiple requests from separate clients
        replicateM_ nRequests $
          spawnLocal $ do
            -- We don't actually care about the timeout, since the message will make it to
            -- the other side regardless. However, we do need to wait for the transfer
            -- to happen, hence why a 50ms timeout
            runClient (void $ request 50_000 serverNodeId ())
            liftIO $ atomically $ do
              n <- readTVar requestsDone
              writeTVar requestsDone (succ n)
