{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

module Test.Control.Distributed.Process.Raft (tests) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (withAsync)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, readMVar)
import Control.Distributed.Process (NodeId (NodeId), spawnLocal)
import Control.Distributed.Process.Node (LocalNode (localNodeId), initRemoteTable, newLocalNode, runProcess)
import Control.Distributed.Process.Raft (networking)
import Control.Distributed.Process.Raft.Admin (getClusterConfiguration, joinCluster, leaveCluster, shutDown, withRaftAdmin)
import Control.Distributed.Process.Raft.Client (request, withRaftClient)
import Control.Monad (replicateM_, void)
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
  let serverNodeId = localNodeId node
  withAsync (sendAdminCommands t serverNodeId) $ \_ -> runProcess node $ do
    serverNetwork <- networking @() @() @()
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

    sendAdminCommands transport serverNodeId = do
      node <- newLocalNode transport initRemoteTable
      runProcess node $ withRaftAdmin $ \runAdmin -> runAdmin $ do
        -- Since the helper functions e.g. 'joinCluster' automatically wait for a response,
        -- we spin up a new thread for each of them
        void $ joinCluster serverNodeId arbitraryOtherNodeId
        void $ leaveCluster serverNodeId
        void $ getClusterConfiguration serverNodeId
        void $ shutDown serverNodeId

testClientCommands :: IO Transport -> TestTree
testClientCommands getTransport = testCase "Tests that client commands are communicated" $ do
  t <- getTransport
  node <- newLocalNode t initRemoteTable
  let serverNodeId = localNodeId node
  requestsVar <- newEmptyMVar
  withAsync (sendClientCommands t serverNodeId) $ \_ -> runProcess node $ do
    serverNetwork <- networking @() @() @()
    -- We give enough time for client requests from other threads
    -- to make it to the server.
    liftIO $ threadDelay 50_000
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
    sendClientCommands transport serverNodeId = do
      node <- newLocalNode transport initRemoteTable
      runProcess node $ withRaftClient @() @() $ \runClient -> do
        -- To demonstrate that pipelining works, we send multiple requests from separate clients
        replicateM_ 3 $
          spawnLocal (runClient (void $ request serverNodeId ()))
