{-# LANGUAGE NumericUnderscores #-}

module Main (main) where

import Control.Applicative ((<**>))
import Control.Concurrent (threadDelay)
import Control.Distributed.Process
  ( NodeId (NodeId),
    Process,
    expect,
    getSelfNode,
    getSelfPid,
    newChan,
    nsendRemote,
    receiveChanTimeout,
    register,
    say,
    sendChan,
  )
import Control.Distributed.Process.Node (initRemoteTable, newLocalNode, runProcess)
import Control.Distributed.STM
  ( Cluster,
    DVar,
    atomically,
    modifyDVar,
    newDVar,
    readDVar,
    retry,
    withCluster,
  )
import Control.Exception (throwIO)
import Control.Monad (forM_, unless)
import Control.Monad.IO.Class (liftIO)
import qualified Data.IntMap.Strict as IntMap
import qualified Data.List as List
import GHC.IO.Handle (BufferMode (LineBuffering), hSetBuffering)
import Network.Socket (HostName)
import Network.Transport.TCP (createTransport, defaultTCPAddr, defaultTCPParameters)
import Network.Transport.TCP.Internal (encodeEndPointAddress)
import Options.Applicative
  ( Alternative (many),
    ParserInfo,
    execParser,
    fullDesc,
    header,
    help,
    helper,
    info,
    long,
    metavar,
    progDesc,
    strOption,
    switch,
  )
import System.IO (stdout)
import System.Random.Stateful (randomRIO)

data RuntimeOpts
  = RuntimeOpts
  { lead :: Bool,
    port :: HostName,
    peers :: [HostName]
  }

parseRuntimeOpts :: ParserInfo RuntimeOpts
parseRuntimeOpts =
  info
    (runtimeOpts <**> helper)
    (fullDesc <> progDesc "Run a dstm bank node" <> header "dstm-bank -- a bank based on distributed software transactional memory")
  where
    runtimeOpts =
      RuntimeOpts
        <$> switch (long "leader")
        <*> strOption
          ( long "port"
              <> metavar "PORT"
              <> help "Port on which this server listens"
          )
        <*> many
          ( strOption
              ( long "peer"
                  <> metavar "PORT"
                  <> help "Port on which a peer listens"
              )
          )

type Account = DVar Int

main :: IO ()
main = do
  hSetBuffering stdout LineBuffering

  opts <- execParser parseRuntimeOpts
  t <- either throwIO pure =<< createTransport (defaultTCPAddr "127.0.0.1" (port opts)) defaultTCPParameters
  node <- newLocalNode t initRemoteTable

  let ps = [NodeId (encodeEndPointAddress "127.0.0.1" p 0) | p <- peers opts]

  runProcess node $ withCluster ps $ \cluster -> do
    getSelfPid >>= register "sync-accounts"
    (acc0, acc1, acc2) <-
      if lead opts
        then do
          accounts <- createAccounts cluster
          (ackPort, acks) <- newChan

          -- Broadcast the new accounts until all other nodes have acknowledged
          let broadcast [] = pure ()
              broadcast pending = do
                forM_ pending $ \otherNode -> nsendRemote otherNode "sync-accounts" (ackPort, accounts)
                acked <- receiveChanTimeout 100_000 acks
                broadcast (maybe pending (`List.delete` pending) acked)

          broadcast ps
          pure accounts
        else do
          (ackPort, accounts) <- expect
          getSelfNode >>= sendChan ackPort
          pure accounts

    -- The core logic is that we wait some random amount of time,
    -- then we transfer 1 dollar from account 'n' to 'n + 1' (modulo 3),
    -- and then we transfer 2 dollars from account 'n' to 'n + 2' (modulo 3).
    --
    -- If account 'n' doesn't have 3 dollars, we retry.
    --
    -- Once the loop completes, we move 'n' to 'n + 1'
    let accountMap = IntMap.fromList [(0, acc0), (1, acc1), (2, acc2)]

        loop n = do
          liftIO $ randomRIO (100_000, 200_000) >>= threadDelay
          (v0, v1, v2, acctSum) <- atomically cluster $ do
            let sourceVar = accountMap IntMap.! (n `mod` 3)
            source <- readDVar sourceVar
            unless (source >= 3) retry
            let addOneVar = accountMap IntMap.! ((n + 1) `mod` 3)
                addTwoVar = accountMap IntMap.! ((n + 2) `mod` 3)
            modifyDVar sourceVar (\x -> x - 3)
            modifyDVar addOneVar (+ 1)
            modifyDVar addTwoVar (+ 2)

            v0 <- readDVar acc0
            v1 <- readDVar acc1
            v2 <- readDVar acc2
            pure (v0, v1, v2, v0 + v1 + v2)

          say $
            mconcat
              [ "Account sum: ",
                show acctSum,
                " (",
                show v0,
                " + ",
                show v1,
                " + ",
                show v2,
                ")"
              ]

          loop (succ n)

    loop 0

createAccounts :: Cluster -> Process (Account, Account, Account)
createAccounts cs = atomically cs $ do
  (,,)
    <$> newDVar 10
    <*> newDVar 10
    <*> newDVar 10
