{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE RankNTypes #-}

module Control.Distributed.STM
  ( DSTM,
    Atomically,
    withDSTM,
    withDSTMOpts,

    -- * DVars
    DVar,
    newDVar,
    writeDVar,
    readDVar,
    modifyDVar,

    -- * Control
    retry,
    orElse,
  )
where

import Control.Concurrent (threadDelay)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, readMVar)
import qualified Control.Concurrent.STM as STM
import Control.Distributed.Process (NodeId, Process, getSelfNode, link, spawnLocal)
import qualified Control.Distributed.Process.Raft as Process
import Control.Distributed.Process.Raft.Client (clientSession, newRequest, sendClientRequest, withRaftClient)
import Control.Distributed.STM.Internal
  ( DVarId (..),
    Store,
    TxCommand (..),
    TxResult (..),
    Version,
    Write (..),
    WriteError (..),
    emptyStore,
    txEngine,
    writePayload,
  )
import Control.Exception (Exception, SomeException (SomeException), throwIO)
import Control.Monad
import Control.Monad.Except (ExceptT, MonadError, catchError, runExceptT, throwError)
import Control.Monad.Reader (MonadIO (liftIO), MonadReader (ask), MonadTrans (lift), ReaderT, asks, lift, runReaderT)
import Control.Monad.State.Strict (MonadState, StateT, get, gets, modify', runStateT)
import Data.Binary (Binary, Word64)
import qualified Data.Binary as Binary
import Data.ByteString (StrictByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import Data.Functor ((<&>))
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.Maybe (fromMaybe, listToMaybe)
import qualified Data.Set as Set
import Distributed.Consensus.Raft (ClusterState (..), Config (..), Implementation (..), Persistence, SessionId, ephemeralPersistence, nullTracer, runRaftServer, simpleConfig)
import Distributed.Consensus.Raft.Client (ClientError (..))
import GHC.Generics (Generic)

data Cluster = MkCluster
  { call :: TxCommand -> Process TxResult,
    session :: SessionId,
    nextLocalId :: STM.TVar Word64
  }

-- | Type synonym for a function which can run a 'DSTM' action.
type Atomically = forall a. DSTM a -> Process a

-- | Spawn all of the services needed to form a distributed STM cluster.
--
-- @
-- 'withDSTM' peers $ \atomically -> do
--     counter <- atomically $ 'newDVar' (0 :: Int)
--     atomically $ 'modifyDVar' counter succ
-- @
--
-- 'withDSTM' will use sensible defaults for the underlying Raft cluster,
-- while disabling fault-tolerance. If you need to enable fault-tolerance,
-- or tune performance characteristics, see 'withDSTMOpts'
withDSTM ::
  [NodeId] ->
  (Atomically -> Process r) ->
  Process r
withDSTM =
  withDSTMOpts
    -- TODO: ephemeralPersistence only makes sense if
    -- the 'NodeId' changed on restart, which it does not.
    --
    -- Before this is turned into a package, ephemeralPersistence
    -- must be swapped for something else.
    ephemeralPersistence
    id

withDSTMOpts ::
  Persistence TxCommand NodeId Store Process ->
  (Config NodeId -> Config NodeId) ->
  [NodeId] ->
  (Atomically -> Process r) ->
  Process r
withDSTMOpts persist localConfig peers body = do
  spawnLocal (serverProcess persist localConfig peers) >>= link
  nid <- getSelfNode
  cs <- cluster (fromMaybe nid $ listToMaybe peers) (nid : peers)
  body (runAtomically cs)

serverProcess ::
  Persistence TxCommand NodeId Store Process ->
  (Config NodeId -> Config NodeId) ->
  [NodeId] ->
  Process ()
serverProcess persist localConfig peers = do
  let clusterState = InCluster (Set.fromList peers)
  network <- Process.networking
  nid <- getSelfNode
  conf <- simpleConfig nid
  runRaftServer
    (localConfig conf)
    clusterState
    emptyStore
    ( Implementation
        { persistence = persist,
          stateMachine = txEngine,
          networking = network,
          tracer = nullTracer
        }
    )

cluster :: NodeId -> [NodeId] -> Process Cluster
cluster leader known = do
  clientVar <- liftIO newEmptyMVar
  clientSessionProcess <- spawnLocal $ do
    withRaftClient $ \run -> do
      sid <- run clientSession
      liftIO (putMVar clientVar (run, sid))
      forever $ liftIO (threadDelay 1_000_000)

  link clientSessionProcess

  (run, sess) <- liftIO (readMVar clientVar)

  -- We add logic to run redirects and keep track of
  -- current leader here
  currentLeader <- liftIO (STM.newTVarIO leader)
  let callClient command = run $ do
        -- We re-use the same request until success. In the future,
        -- client requests will be de-duplicated, in which case
        -- re-using the request buys us safety for free
        req <- newRequest command
        let send = do
              lid <- lift . liftIO $ STM.readTVarIO currentLeader
              sendClientRequest 1_000_000 lid req
                >>= \case
                  Left (Redirect actualLeader) -> do
                    lift $ liftIO $ STM.atomically $ STM.writeTVar currentLeader actualLeader
                    send
                  Left NoKnownLeader -> do
                    -- TODO: proper retrying
                    lift $ liftIO (threadDelay 100_000)
                    send
                  Left Timeout -> do
                    -- Rotate through known nodes to find another to contact
                    let next = fromMaybe lid $ listToMaybe $ drop 1 (dropWhile (/= lid) known) ++ known
                    lift $ liftIO $ STM.atomically $ STM.writeTVar currentLeader next
                    send
                  Right (actualLeader, x) -> do
                    lift $ liftIO $ STM.atomically $ STM.writeTVar currentLeader actualLeader
                    pure x

        send

  MkCluster callClient sess <$> liftIO (STM.newTVarIO 0)

data TxLog = MkTxLog
  { txReads :: Map DVarId (Version, StrictByteString),
    txWrites :: Map DVarId Write
  }

data Signal
  = Retry
  | Failure SomeException
  deriving (Show)

instance Exception Signal

newtype BadDecode = BadDecode String
  deriving (Show)

instance Exception BadDecode

newtype InternalError = InternalError String
  deriving (Show)

instance Exception InternalError

fetch :: Cluster -> Map DVarId Version -> DVarId -> Process (Maybe (Version, StrictByteString))
fetch cs readSet did =
  call cs (Fetch readSet did) >>= \case
    Value v bs -> pure (Just (v, bs))
    Conflict {} -> pure Nothing
    Rejected errs -> rejected errs
    Committed -> liftIO $ throwIO $ Failure (SomeException $ InternalError "Unexpected 'Committed' response")

commit :: Cluster -> Map DVarId Version -> Map DVarId Write -> Process Bool
commit cs readSet writeSet =
  call cs (Commit readSet writeSet) >>= \case
    Committed -> pure True
    Conflict {} -> pure False
    Rejected errs -> rejected errs
    Value {} -> liftIO $ throwIO $ Failure (SomeException $ InternalError "Unexpected 'Value' response")

rejected :: [(DVarId, WriteError)] -> Process a
rejected errs = liftIO $ throwIO $ Failure $ SomeException $ case errs of
  (_did, NoSuchDVar) : _ -> InternalError "No such DVar"
  _ -> InternalError "Rejected"

newtype DSTM a
  = DSTM (ExceptT Signal (StateT TxLog (ReaderT Cluster Process)) a)
  deriving newtype
    ( Functor,
      Applicative,
      Monad,
      MonadReader Cluster,
      MonadState TxLog,
      MonadError Signal
    )

liftProcess :: Process a -> DSTM a
liftProcess = DSTM . lift . lift . lift

runAtomically :: Cluster -> DSTM a -> Process a
runAtomically cs (DSTM body) = attempt
  where
    attempt = do
      (r, tx) <- runReaderT (runStateT (runExceptT body) (MkTxLog mempty mempty)) cs
      case r of
        -- TODO: performance optimization by awaiting a change in one of the DVars
        --       involved in 'tx'.
        --       I think this would require polling the server
        Left (Failure exc) -> liftIO $ throwIO exc
        Left Retry -> attempt
        Right a
          | Map.null (txWrites tx) -> pure a
          | otherwise -> do
              committed <- commit cs (Map.map fst (txReads tx)) (txWrites tx)
              if committed
                then pure a
                else attempt

newtype DVar a = DVar DVarId
  deriving (Show, Generic)

instance Binary (DVar a)

newDVar :: (Binary a) => a -> DSTM (DVar a)
newDVar x = do
  c <- ask
  localIdVar <- asks nextLocalId
  counter <- DSTM $ liftIO $ STM.readTVarIO localIdVar
  let tid = MkDVarId (session c) counter
  modify' $ \tx -> tx {txWrites = Map.insert tid (Create (BS.toStrict $ Binary.encode x)) (txWrites tx)}
  DSTM $ liftIO $ STM.atomically $ STM.modifyTVar' localIdVar succ
  pure $ DVar tid

writeDVar :: (Binary a) => DVar a -> a -> DSTM ()
writeDVar (DVar tid) x = modify' $ \tx -> tx {txWrites = Map.alter put tid (txWrites tx)}
  where
    bytes = BS.toStrict (Binary.encode x)
    -- This might seem strange, but recall that 'Create' here means we created
    -- the variable in this transaction, which is fine
    put (Just (Create {})) = Just (Create bytes)
    put _ = Just (Update bytes)

readDVar :: (Binary a) => DVar a -> DSTM a
readDVar (DVar tid) = do
  tx <- get
  bytes <- case (Map.lookup tid (txWrites tx), Map.lookup tid (txReads tx)) of
    (Just w, _) -> pure (writePayload w)
    (_, Just (_, bs)) -> pure bs
    _ -> do
      cs <- ask
      liftProcess (fetch cs (Map.map fst (txReads tx)) tid) >>= \case
        Just (v, bs) -> bs <$ modify' (\t -> t {txReads = Map.insert tid (v, bs) (txReads t)})
        Nothing -> throwError Retry

  either
    (\(_, _, errmsg) -> throwError (Failure (SomeException (BadDecode errmsg))))
    ( \(unconsumed, _, decoded) ->
        if BSL.null unconsumed
          then pure decoded
          else
            throwError (Failure (SomeException (BadDecode "Input not fully consumed")))
    )
    $ Binary.decodeOrFail (BS.fromStrict bytes)

modifyDVar :: (Binary a) => DVar a -> (a -> a) -> DSTM ()
modifyDVar v f = readDVar v <&> f >>= writeDVar v

retry :: DSTM a
retry = throwError Retry

orElse :: DSTM a -> DSTM a -> DSTM a
orElse left right = do
  saved <- gets txWrites
  left `catchError` \case
    Retry -> modify' (\tx -> tx {txWrites = saved}) >> right
    e -> throwError e
