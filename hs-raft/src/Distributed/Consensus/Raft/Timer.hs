{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Distributed.Consensus.Raft.Timer
  ( Microseconds (Microseconds),
    Timer,
    newTimer,
    resetTimer,
    cancelTimer,
  )
where

import Control.Concurrent.Class.MonadMVar (MVar, MonadMVar, newEmptyMVar, putMVar, tryTakeMVar)
import Control.Monad.Class.MonadAsync (Async, MonadAsync, async, cancel, link)
import Control.Monad.Class.MonadFork (MonadFork)
import Control.Monad.Class.MonadThrow (MonadMask)
import Control.Monad.Class.MonadTimer (MonadDelay, threadDelay)
import Data.Foldable (traverse_)
import Data.Int (Int32)
import GHC.Generics (Generic)
import System.Random (UniformRange)

-- | A 'Timer' is an action which is run after some amount of time.
--
-- When a 'Timer' is created with 'newTimer', the action is registered
-- but the timer is inactive. Use 'resetTimer' to launch the coundown.
-- Once the countdown finishes, the action is executed.
--
-- You can reset the timer by using 'resetTimer' again.
--
-- If the action throws an exception, the exception will be thrown
-- in the thread that created the timer with 'newTimer'.
data Timer m = MkTimer
  { _action :: m (),
    -- Normally we would use an IORef (Maybe _) here,
    -- but 'io-classes' doesn't support IORef
    _handle :: MVar m (Async m ())
  }

newtype Microseconds = Microseconds Int32
  deriving stock (Eq, Ord, Show, Generic)
  deriving newtype (Real, Enum, Num, Integral, Bounded)

instance UniformRange Microseconds -- via Generic

newTimer :: (MonadMVar m) => m () -> m (Timer m)
newTimer a = MkTimer a <$> newEmptyMVar

resetTimer ::
  (MonadAsync m, MonadMask m, MonadFork m, MonadMVar m, MonadDelay m) =>
  Microseconds -> Timer m -> m ()
resetTimer (Microseconds delay) timer@(MkTimer a h) = do
  cancelTimer timer
  thread <- async (threadDelay (fromIntegral delay) >> a)
  link thread -- can't be too careful
  putMVar h thread

cancelTimer :: (MonadMVar m, MonadAsync m) => Timer m -> m ()
cancelTimer (MkTimer _ h) =
  tryTakeMVar h >>= traverse_ cancel
