{-# LANGUAGE CPP #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeFamilies #-}
{-# OPTIONS_GHC -Wno-orphans #-}

{- HLINT ignore "Avoid lambda" -}

module Control.Distributed.Process.Raft.Instances () where

import Control.Applicative (Alternative)
import Control.Concurrent qualified as IO
import Control.Concurrent.Async qualified as Async
import Control.Concurrent.Class.MonadMVar
  ( MonadInspectMVar (..),
    MonadLabelledMVar (..),
    MonadMVar (..),
    MonadTraceMVar (..),
  )
import Control.Concurrent.STM.TArray qualified as STM
import Control.Concurrent.STM.TBQueue qualified as STM
import Control.Concurrent.STM.TChan qualified as STM
import Control.Concurrent.STM.TMVar qualified as STM
import Control.Concurrent.STM.TQueue qualified as STM
import Control.Concurrent.STM.TSem qualified as STM
import Control.Concurrent.STM.TVar qualified as STM
import Control.Distributed.Process.Internal.Types
  ( Process (..),
    runLocalProcess,
  )
import Control.Exception qualified as IO
#if __GLASGOW_HASKELL__ >= 910
import Control.Exception.Annotation (ExceptionAnnotation)
#endif
import Control.Monad (MonadPlus)
import Control.Monad.Class.MonadAsync (MonadAsync (..))
import Control.Monad.Class.MonadEventlog (MonadEventlog (..))
import Control.Monad.Class.MonadFork
  ( MonadFork (..),
    MonadThread (..),
  )
import Control.Monad.Class.MonadST (MonadST (..))
import Control.Monad.Class.MonadSTM.Internal
  ( MonadInspectSTM (..),
    MonadLabelledSTM (..),
    MonadSTM (..),
    MonadTraceSTM (..),
  )
import Control.Monad.Class.MonadSay (MonadSay (..))
import Control.Monad.Class.MonadTest (MonadTest)
import Control.Monad.Class.MonadThrow
  ( ExitCase (ExitCaseException, ExitCaseSuccess),
    MonadCatch (..),
    MonadEvaluate (..),
    MonadMask (..),
    MonadThrow (..),
  )
import Control.Monad.Class.MonadTime
  ( MonadMonotonicTimeNSec (..),
    MonadTime (..),
  )
import Control.Monad.Class.MonadTime.SI qualified as SI
import Control.Monad.Class.MonadTimer
  ( MonadDelay (..),
    MonadTimer (..),
  )
import Control.Monad.Class.MonadTimer.SI qualified as SI
import Control.Monad.Class.MonadUnique (MonadUnique (..))
import Control.Monad.Fix (MonadFix)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Primitive (PrimMonad (..), RealWorld, stToPrim)
import Control.Monad.STM qualified as STM
import Control.Monad.Trans.Reader (ReaderT (..))
import Data.Array.Base (MArray (..))
import Data.Bifunctor (first)
import Data.Unique qualified as IO
import GHC.Conc.Sync qualified as IO (labelThread, threadLabel)
import GHC.Stack (HasCallStack)
import System.Timeout qualified as IO

withRunInIO :: ((forall x. Process x -> IO x) -> IO a) -> Process a
withRunInIO k = Process (ReaderT (\lproc -> k (runLocalProcess lproc)))
{-# INLINE withRunInIO #-}

liftRestore ::
  (forall x. IO x -> IO x) ->
  (forall x. Process x -> Process x)
liftRestore restore p =
  Process (ReaderT (\lproc -> restore (runLocalProcess lproc p)))
{-# INLINE liftRestore #-}

atomicallyIO :: (HasCallStack) => STM.STM a -> IO a
atomicallyIO = atomically
{-# INLINE atomicallyIO #-}

instance MonadThrow Process where
  throwIO = liftIO . IO.throwIO

  bracket acquire release use = withRunInIO $ \run ->
    IO.bracket (run acquire) (run . release) (run . use)
  bracket_ before after thing = withRunInIO $ \run ->
    IO.bracket_ (run before) (run after) (run thing)
  finally thing sequel = withRunInIO $ \run ->
    IO.finally (run thing) (run sequel)

#if __GLASGOW_HASKELL__ >= 910
  annotateIO :: forall e a. (ExceptionAnnotation e) => e -> Process a -> Process a
  annotateIO ann p = withRunInIO $ \run -> IO.annotateIO ann (run p)
#endif

instance MonadCatch Process where
  catch action handler = withRunInIO $ \run ->
    IO.catch (run action) (run . handler)

  catchJust p action handler = withRunInIO $ \run ->
    IO.catchJust p (run action) (run . handler)
  try action = withRunInIO $ \run -> IO.try (run action)
  tryJust p action = withRunInIO $ \run -> IO.tryJust p (run action)
  handle handler action = withRunInIO $ \run ->
    IO.handle (run . handler) (run action)
  handleJust p handler action = withRunInIO $ \run ->
    IO.handleJust p (run . handler) (run action)
  onException action what = withRunInIO $ \run ->
    IO.onException (run action) (run what)
  bracketOnError acquire release use = withRunInIO $ \run ->
    IO.bracketOnError (run acquire) (run . release) (run . use)

instance MonadMask Process where
  mask action = withRunInIO $ \run ->
    IO.mask (\restore -> run (action (liftRestore restore)))
  mask_ action = withRunInIO $ \run -> IO.mask_ (run action)

  uninterruptibleMask action = withRunInIO $ \run ->
    IO.uninterruptibleMask (\restore -> run (action (liftRestore restore)))
  uninterruptibleMask_ action = withRunInIO $ \run ->
    IO.uninterruptibleMask_ (run action)

  getMaskingState = liftIO IO.getMaskingState
  interruptible action = withRunInIO $ \run -> IO.interruptible (run action)
  allowInterrupt = liftIO IO.allowInterrupt

instance MonadEvaluate Process where
  evaluate = liftIO . IO.evaluate

newtype ProcessSTM a = ProcessSTM {runProcessSTM :: STM.STM a}
  deriving newtype (Functor, Applicative, Monad, Alternative, MonadPlus, MonadFix)

deriving newtype instance (Semigroup a) => Semigroup (ProcessSTM a)

deriving newtype instance (Monoid a) => Monoid (ProcessSTM a)

instance MonadThrow ProcessSTM where
  throwIO = ProcessSTM . STM.throwSTM

#if __GLASGOW_HASKELL__ >= 910
  annotateIO ::
    forall e a.
    (ExceptionAnnotation e) =>
    e -> ProcessSTM a -> ProcessSTM a
  annotateIO ann stm =
    stm `catch` \e -> throwIO (IO.addExceptionContext ann e)
#endif

instance MonadCatch ProcessSTM where
  catch action handler =
    ProcessSTM (STM.catchSTM (runProcessSTM action) (runProcessSTM . handler))

  generalBracket acquire release use = do
    resource <- acquire
    b <-
      use resource `catch` \e -> do
        _ <- release resource (ExitCaseException e)
        throwIO e
    c <- release resource (ExitCaseSuccess b)
    return (b, c)

instance MArray STM.TArray e ProcessSTM where
  getBounds = ProcessSTM . getBounds
  getNumElements = ProcessSTM . getNumElements
  unsafeRead arr = ProcessSTM . unsafeRead arr
  unsafeWrite arr i = ProcessSTM . unsafeWrite arr i
  newArray idxs = ProcessSTM . newArray idxs

instance MonadSTM Process where
  type STM Process = ProcessSTM

  atomically :: forall a. (HasCallStack) => ProcessSTM a -> Process a
  atomically = liftIO . atomicallyIO . runProcessSTM

  type TVar Process = STM.TVar
  type TMVar Process = STM.TMVar
  type TQueue Process = STM.TQueue
  type TBQueue Process = STM.TBQueue
  type TArray Process = STM.TArray
  type TSem Process = STM.TSem
  type TChan Process = STM.TChan

  newTVar = ProcessSTM . STM.newTVar
  readTVar = ProcessSTM . STM.readTVar
  writeTVar v = ProcessSTM . STM.writeTVar v
  retry = ProcessSTM STM.retry
  orElse a = ProcessSTM . STM.orElse (runProcessSTM a) . runProcessSTM
  modifyTVar v = ProcessSTM . STM.modifyTVar v
  modifyTVar' v = ProcessSTM . STM.modifyTVar' v
  stateTVar v = ProcessSTM . STM.stateTVar v
  swapTVar v = ProcessSTM . STM.swapTVar v
  check = ProcessSTM . STM.check

  newTMVar = ProcessSTM . STM.newTMVar
  newEmptyTMVar = ProcessSTM STM.newEmptyTMVar
  takeTMVar = ProcessSTM . STM.takeTMVar
  tryTakeTMVar = ProcessSTM . STM.tryTakeTMVar
  putTMVar v = ProcessSTM . STM.putTMVar v
  tryPutTMVar v = ProcessSTM . STM.tryPutTMVar v
  readTMVar = ProcessSTM . STM.readTMVar
  tryReadTMVar = ProcessSTM . STM.tryReadTMVar
  swapTMVar v = ProcessSTM . STM.swapTMVar v
  writeTMVar v = ProcessSTM . STM.writeTMVar v
  isEmptyTMVar = ProcessSTM . STM.isEmptyTMVar

  newTQueue = ProcessSTM STM.newTQueue
  readTQueue = ProcessSTM . STM.readTQueue
  tryReadTQueue = ProcessSTM . STM.tryReadTQueue
  peekTQueue = ProcessSTM . STM.peekTQueue
  tryPeekTQueue = ProcessSTM . STM.tryPeekTQueue
  flushTQueue = ProcessSTM . STM.flushTQueue
  writeTQueue v = ProcessSTM . STM.writeTQueue v
  isEmptyTQueue = ProcessSTM . STM.isEmptyTQueue
  unGetTQueue v = ProcessSTM . STM.unGetTQueue v

  newTBQueue = ProcessSTM . STM.newTBQueue
  readTBQueue = ProcessSTM . STM.readTBQueue
  tryReadTBQueue = ProcessSTM . STM.tryReadTBQueue
  peekTBQueue = ProcessSTM . STM.peekTBQueue
  tryPeekTBQueue = ProcessSTM . STM.tryPeekTBQueue
  flushTBQueue = ProcessSTM . STM.flushTBQueue
  writeTBQueue v = ProcessSTM . STM.writeTBQueue v
  lengthTBQueue = ProcessSTM . STM.lengthTBQueue
  isEmptyTBQueue = ProcessSTM . STM.isEmptyTBQueue
  isFullTBQueue = ProcessSTM . STM.isFullTBQueue
  unGetTBQueue v = ProcessSTM . STM.unGetTBQueue v

  newTSem = ProcessSTM . STM.newTSem
  waitTSem = ProcessSTM . STM.waitTSem
  signalTSem = ProcessSTM . STM.signalTSem
  signalTSemN n = ProcessSTM . STM.signalTSemN n

  newTChan = ProcessSTM STM.newTChan
  newBroadcastTChan = ProcessSTM STM.newBroadcastTChan
  dupTChan = ProcessSTM . STM.dupTChan
  cloneTChan = ProcessSTM . STM.cloneTChan
  readTChan = ProcessSTM . STM.readTChan
  tryReadTChan = ProcessSTM . STM.tryReadTChan
  peekTChan = ProcessSTM . STM.peekTChan
  tryPeekTChan = ProcessSTM . STM.tryPeekTChan
  writeTChan v = ProcessSTM . STM.writeTChan v
  unGetTChan v = ProcessSTM . STM.unGetTChan v
  isEmptyTChan = ProcessSTM . STM.isEmptyTChan

  newTVarIO = liftIO . STM.newTVarIO
  readTVarIO = liftIO . STM.readTVarIO
  newTMVarIO = liftIO . STM.newTMVarIO
  newEmptyTMVarIO = liftIO STM.newEmptyTMVarIO
  newTQueueIO = liftIO STM.newTQueueIO
  newTBQueueIO = liftIO . STM.newTBQueueIO
  newTChanIO = liftIO STM.newTChanIO
  newBroadcastTChanIO = liftIO STM.newBroadcastTChanIO

instance MonadLabelledSTM Process where
  labelTVar _ _ = return ()
  labelTMVar _ _ = return ()
  labelTQueue _ _ = return ()
  labelTBQueue _ _ = return ()
  labelTArray _ _ = return ()
  labelTSem _ _ = return ()
  labelTChan _ _ = return ()

  labelTVarIO _ _ = return ()
  labelTMVarIO _ _ = return ()
  labelTQueueIO _ _ = return ()
  labelTBQueueIO _ _ = return ()
  labelTArrayIO _ _ = return ()
  labelTSemIO _ _ = return ()
  labelTChanIO _ _ = return ()

instance MonadInspectSTM Process where
  type InspectMonadSTM Process = IO
  inspectTVar _ = STM.readTVarIO
  inspectTMVar _ = STM.atomically . STM.tryReadTMVar

instance MonadTraceSTM Process where
  traceTVar _ _ _ = return ()
  traceTMVar _ _ _ = return ()
  traceTQueue _ _ _ = return ()
  traceTBQueue _ _ _ = return ()
  traceTSem _ _ _ = return ()

  traceTVarIO _ _ = return ()
  traceTMVarIO _ _ = return ()
  traceTQueueIO _ _ = return ()
  traceTBQueueIO _ _ = return ()
  traceTSemIO _ _ = return ()

instance MonadMVar Process where
  type MVar Process = IO.MVar

  newEmptyMVar = liftIO IO.newEmptyMVar
  newMVar = liftIO . IO.newMVar
  takeMVar = liftIO . IO.takeMVar
  putMVar v = liftIO . IO.putMVar v
  readMVar = liftIO . IO.readMVar
  swapMVar v = liftIO . IO.swapMVar v
  tryTakeMVar = liftIO . IO.tryTakeMVar
  tryPutMVar v = liftIO . IO.tryPutMVar v
  tryReadMVar = liftIO . IO.tryReadMVar
  isEmptyMVar = liftIO . IO.isEmptyMVar

  withMVar v f = withRunInIO $ \run -> IO.withMVar v (run . f)
  withMVarMasked v f = withRunInIO $ \run -> IO.withMVarMasked v (run . f)
  modifyMVar_ v f = withRunInIO $ \run -> IO.modifyMVar_ v (run . f)
  modifyMVar v f = withRunInIO $ \run -> IO.modifyMVar v (run . f)
  modifyMVarMasked_ v f = withRunInIO $ \run -> IO.modifyMVarMasked_ v (run . f)
  modifyMVarMasked v f = withRunInIO $ \run -> IO.modifyMVarMasked v (run . f)

instance MonadInspectMVar Process where
  type InspectMVarMonad Process = IO
  inspectMVar _ = IO.tryReadMVar

instance MonadTraceMVar Process where
  traceMVarIO _ _ _ = return ()

instance MonadLabelledMVar Process where
  labelMVar _ _ = return ()

instance MonadThread Process where
  type ThreadId Process = IO.ThreadId
  myThreadId = liftIO IO.myThreadId
  labelThread t = liftIO . IO.labelThread t
  threadLabel = liftIO . IO.threadLabel

instance MonadFork Process where
  forkIO p = withRunInIO $ \run -> IO.forkIO (run p)
  forkOn n p = withRunInIO $ \run -> IO.forkOn n (run p)

  forkIOWithUnmask k = withRunInIO $ \run ->
    IO.forkIOWithUnmask (\unmask -> run (k (liftRestore unmask)))

  forkFinally p k = withRunInIO $ \run -> IO.forkFinally (run p) (run . k)

  throwTo t = liftIO . IO.throwTo t
  killThread = liftIO . IO.killThread
  yield = liftIO IO.yield
  getNumCapabilities = liftIO IO.getNumCapabilities

newtype ProcessAsync a = ProcessAsync {runProcessAsync :: Async.Async a}
  deriving (Eq, Ord, Functor)

instance MonadAsync Process where
  type Async Process = ProcessAsync

  async p = withRunInIO $ \run -> ProcessAsync <$> Async.async (run p)
  asyncBound p = withRunInIO $ \run -> ProcessAsync <$> Async.asyncBound (run p)
  asyncOn n p = withRunInIO $ \run -> ProcessAsync <$> Async.asyncOn n (run p)

  asyncThreadId = Async.asyncThreadId . runProcessAsync
  compareAsyncs a b =
    Async.compareAsyncs (runProcessAsync a) (runProcessAsync b)

  asyncWithUnmask k = withRunInIO $ \run ->
    ProcessAsync
      <$> Async.asyncWithUnmask
        (\unmask -> run (k (liftRestore unmask)))
  asyncOnWithUnmask n k = withRunInIO $ \run ->
    ProcessAsync
      <$> Async.asyncOnWithUnmask
        n
        (\unmask -> run (k (liftRestore unmask)))

  withAsync p f = withRunInIO $ \run ->
    Async.withAsync (run p) (run . f . ProcessAsync)
  withAsyncBound p f = withRunInIO $ \run ->
    Async.withAsyncBound (run p) (run . f . ProcessAsync)
  withAsyncOn n p f = withRunInIO $ \run ->
    Async.withAsyncOn n (run p) (run . f . ProcessAsync)

  withAsyncWithUnmask k f = withRunInIO $ \run ->
    Async.withAsyncWithUnmask
      (\unmask -> run (k (liftRestore unmask)))
      (run . f . ProcessAsync)
  withAsyncOnWithUnmask n k f = withRunInIO $ \run ->
    Async.withAsyncOnWithUnmask
      n
      (\unmask -> run (k (liftRestore unmask)))
      (run . f . ProcessAsync)

  waitSTM = ProcessSTM . Async.waitSTM . runProcessAsync
  pollSTM = ProcessSTM . Async.pollSTM . runProcessAsync
  waitCatchSTM = ProcessSTM . Async.waitCatchSTM . runProcessAsync

  waitAnySTM =
    ProcessSTM
      . fmap (first ProcessAsync)
      . Async.waitAnySTM
      . map runProcessAsync
  waitAnyCatchSTM =
    ProcessSTM
      . fmap (first ProcessAsync)
      . Async.waitAnyCatchSTM
      . map runProcessAsync
  waitEitherSTM a b = ProcessSTM (Async.waitEitherSTM (runProcessAsync a) (runProcessAsync b))
  waitEitherSTM_ a b = ProcessSTM (Async.waitEitherSTM_ (runProcessAsync a) (runProcessAsync b))
  waitEitherCatchSTM a b = ProcessSTM (Async.waitEitherCatchSTM (runProcessAsync a) (runProcessAsync b))
  waitBothSTM a b = ProcessSTM (Async.waitBothSTM (runProcessAsync a) (runProcessAsync b))

  wait = liftIO . Async.wait . runProcessAsync
  poll = liftIO . Async.poll . runProcessAsync
  waitCatch = liftIO . Async.waitCatch . runProcessAsync
  cancel = liftIO . Async.cancel . runProcessAsync
  uninterruptibleCancel = liftIO . Async.uninterruptibleCancel . runProcessAsync
  cancelWith a e = liftIO (Async.cancelWith (runProcessAsync a) e)

  waitAny =
    liftIO
      . fmap (first ProcessAsync)
      . Async.waitAny
      . map runProcessAsync
  waitAnyCatch =
    liftIO
      . fmap (first ProcessAsync)
      . Async.waitAnyCatch
      . map runProcessAsync
  waitAnyCancel =
    liftIO
      . fmap (first ProcessAsync)
      . Async.waitAnyCancel
      . map runProcessAsync
  waitAnyCatchCancel =
    liftIO
      . fmap (first ProcessAsync)
      . Async.waitAnyCatchCancel
      . map runProcessAsync

  waitEither a b = liftIO (Async.waitEither (runProcessAsync a) (runProcessAsync b))
  waitEither_ a b = liftIO (Async.waitEither_ (runProcessAsync a) (runProcessAsync b))
  waitEitherCatch a b = liftIO (Async.waitEitherCatch (runProcessAsync a) (runProcessAsync b))
  waitEitherCancel a b = liftIO (Async.waitEitherCancel (runProcessAsync a) (runProcessAsync b))
  waitEitherCatchCancel a b = liftIO (Async.waitEitherCatchCancel (runProcessAsync a) (runProcessAsync b))
  waitBoth a b = liftIO (Async.waitBoth (runProcessAsync a) (runProcessAsync b))

  race a b = withRunInIO $ \run -> Async.race (run a) (run b)
  race_ a b = withRunInIO $ \run -> Async.race_ (run a) (run b)
  concurrently a b = withRunInIO $ \run -> Async.concurrently (run a) (run b)
  concurrently_ a b = withRunInIO $ \run -> Async.concurrently_ (run a) (run b)

instance MonadDelay Process where
  threadDelay = liftIO . IO.threadDelay

instance MonadTimer Process where
  registerDelay = liftIO . STM.registerDelay
  timeout d p = withRunInIO $ \run -> IO.timeout d (run p)

instance MonadMonotonicTimeNSec Process where
  getMonotonicTimeNSec = liftIO getMonotonicTimeNSec

instance MonadTime Process where
  getCurrentTime = liftIO getCurrentTime

instance SI.MonadMonotonicTime Process

instance SI.MonadDelay Process where
  threadDelay = liftIO . SI.threadDelay

instance SI.MonadTimer Process where
  registerDelay = liftIO . SI.registerDelay

  registerDelayCancellable d = do
    (readTs, cancelTs) <- liftIO (SI.registerDelayCancellable d)
    return (ProcessSTM readTs, liftIO cancelTs)

  timeout d p = withRunInIO $ \run -> SI.timeout d (run p)

instance PrimMonad Process where
  type PrimState Process = RealWorld
  primitive = liftIO . primitive
  {-# INLINE primitive #-}

instance MonadST Process where
  stToIO = stToPrim

newtype ProcessUnique = ProcessUnique {getProcessUnique :: IO.Unique}
  deriving newtype (Eq, Ord)

instance MonadUnique Process where
  type Unique Process = ProcessUnique
  newUnique = liftIO (ProcessUnique <$> IO.newUnique)
  hashUnique = IO.hashUnique . getProcessUnique

instance MonadSay Process where
  say = liftIO . say

instance MonadEventlog Process where
  traceEventIO = liftIO . traceEventIO
  traceMarkerIO = liftIO . traceMarkerIO
  flushEventLog = liftIO flushEventLog

instance MonadTest Process
