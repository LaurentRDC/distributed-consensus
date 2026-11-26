{-# LANGUAGE RankNTypes #-}

module Main where

import Control.Concurrent.Async (async, cancel, forConcurrently_, replicateConcurrently_)
import Control.Concurrent.MVar (MVar, newEmptyMVar, putMVar, readMVar, takeMVar)
import Control.Concurrent.STM (atomically, modifyTVar, newTVarIO, readTVar, retry)
import Control.Distributed.Process (NodeId (..), Process)
import Control.Distributed.Process.Node (closeLocalNode, initRemoteTable, newLocalNode, runProcess)
import Control.Distributed.STM (Atomically, DVar, modifyDVar, newDVar, readDVar, withDSTMOpts)
import qualified Control.Distributed.STM as DSTM
import Control.Exception (throwIO)
import Control.Monad (forM, forever, replicateM, when)
import Control.Monad.IO.Class (liftIO)
import Data.IntMap.Strict (IntMap)
import qualified Data.IntMap.Strict as IntMap
import Distributed.Consensus.Raft (Config (heartBeatTimeout, maxLogLength), ephemeralPersistence)
import Network.Transport (Transport, closeTransport)
import Network.Transport.TCP (createTransport, defaultTCPAddr, defaultTCPParameters)
import Network.Transport.TCP.Internal (encodeEndPointAddress)
import System.Random.Stateful (randomRIO)
import Test.Tasty (localOption, withResource)
import Test.Tasty.Bench (RelStDev (..), TimeMode (WallTime), bench, bgroup, defaultMain, nfIO)

main :: IO ()
main = do
  defaultMain
    [ localOption WallTime $
        -- Only interested in relative orders-of-magnitude
        localOption (RelStDev 0.1) $
          bgroup
            -- This top-level group exists so that we only have to apply
            -- local options once
            "dstm benchmark suite"
            [ bgroup
                "stm"
                [ bench
                    ( mconcat
                        [ "STM baseline (",
                          show nt,
                          " thread(s), ",
                          show nl,
                          " transactions)"
                        ]
                    )
                    (nfIO (baselineSTM nt nl))
                | nt <- [1, 5],
                  nl <- [10, 25]
                ],
              withResource (startCluster 5) stopCluster $ \getNodes ->
                bgroup
                  "dstm (5 nodes)"
                  [ bench
                      (show nl <> " transactions")
                      $ nfIO
                      $ getNodes >>= \(_, ns) ->
                        forConcurrently_ ns $ \n -> putMVar (go n) nl >> takeMVar (done n)
                  | nl <- [10, 25]
                  ]
            ]
    ]

data Node = Node
  {go :: MVar Int, done :: MVar (), stop :: IO ()}

startCluster :: Int -> IO (Transport, [Node])
startCluster numNodes = do
  transport <- either throwIO pure =<< createTransport (defaultTCPAddr "127.0.0.1" "4100") defaultTCPParameters

  accountsVar <- newEmptyMVar
  let nodeId ix = NodeId (encodeEndPointAddress "127.0.0.1" "4100" (fromIntegral ix))

  nodes <- forM (zip (True : repeat False) [0 .. numNodes - 1]) $ \(lead, ix) -> do
    localNode <- newLocalNode transport initRemoteTable
    goVar <- newEmptyMVar
    doneVar <- newEmptyMVar
    tid <- async
      $ runProcess localNode
      $ withDSTMOpts
        ephemeralPersistence
        (\conf -> conf {heartBeatTimeout = 100, maxLogLength = Nothing})
        [nodeId j | j <- [0 .. numNodes - 1], j /= ix]
      $ \datomically -> do
        accounts <-
          if lead
            then do
              accs <- datomically $ IntMap.fromList . zip [0 ..] <$> replicateM 100 (newDVar (10 :: Int))
              accs <$ liftIO (putMVar accountsVar accs)
            else liftIO (readMVar accountsVar)

        liftIO (putMVar doneVar ())
        forever $ do
          liftIO (takeMVar goVar) >>= workloadDSTM datomically accounts
          liftIO (putMVar doneVar ())

    pure $ Node goVar doneVar (closeLocalNode localNode >> cancel tid)

  mapM_ (takeMVar . done) nodes
  pure (transport, nodes)

stopCluster :: (Transport, [Node]) -> IO ()
stopCluster (transport, nodes) = mapM_ stop nodes >> closeTransport transport

baselineSTM :: Int -> Int -> IO ()
baselineSTM numThreads numLoops = do
  accountMap <- IntMap.fromList . zip [0 ..] <$> replicateM 100 (newTVarIO (10 :: Int))
  let mi = minimum $ IntMap.keys accountMap
      ma = maximum $ IntMap.keys accountMap

  let loop n
        | n <= 0 = pure ()
        | otherwise = do
            source <- randomRIO (mi, ma)
            dest <- randomRIO (mi, ma)
            atomically $ do
              let sourceVar = accountMap IntMap.! source
                  destVar = accountMap IntMap.! dest
              sourceAmount <- readTVar sourceVar
              when (sourceAmount == 0) retry
              modifyTVar sourceVar (subtract 1)
              modifyTVar destVar (+ 1)

            loop (pred n)

  replicateConcurrently_ numThreads $ loop numLoops

workloadDSTM :: Atomically -> IntMap (DVar Int) -> Int -> Process ()
workloadDSTM datomically accountMap nLoops = do
  let mi = minimum $ IntMap.keys accountMap
      ma = maximum $ IntMap.keys accountMap

  let loop n
        | n <= 0 = pure ()
        | otherwise = do
            source <- randomRIO (mi, ma)
            dest <- randomRIO (mi, ma)
            datomically $ do
              let sourceVar = accountMap IntMap.! source
                  destVar = accountMap IntMap.! dest
              sourceAmount <- readDVar sourceVar
              when (sourceAmount == 0) DSTM.retry
              modifyDVar sourceVar (subtract 1)
              modifyDVar destVar (+ 1)

            loop (pred n)

  loop nLoops
