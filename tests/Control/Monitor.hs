{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveFunctor #-}

module Control.Monitor
  ( -- * Types
    Monitor,
    Failure (..),
    Concurrently,

    -- ** Running the 'Monitor'
    runMonitor,

    -- * Combinators
    expect,
    eventually,
    never,
    always,
    until,
    weakUntil,
    collectUntil,
    scanUntil,
    both,
    next,
    whenever,
    -- TODO:
    -- within :: DiffTime -> Monitor (Time, event) failure result -> Monitor (Time, event) failure result
    --
    -- For Concurrently:
    -- allOf :: [Monitor e f ()] -> Monitor e f () (runConcurrently . traverse_ Concurrently?)
    -- anyOf :: [Monitor e f a] -> MOnitor e f a (asum?)
    -- race :: Monitor e f a -> Monitor e f b -> Monitor e f (Either a b)
    --
    -- For error handling:
    -- label :: String -> Monitor e f a -> Monitor e f a
  )
where

import Control.Monad ((>=>))
import Data.Bifunctor (first)
import Data.Foldable (traverse_)
import Prelude hiding (until)

data Monitor event failure result
  = Done !result
  | Fail !(Failure event failure)
  | Step
      -- | What to do on end-of-input
      (Either (Failure event failure) result)
      -- | How to continue
      (event -> Monitor event failure result)
  deriving (Functor)

runMonitor ::
  Monitor event failure result ->
  [event] ->
  Either (Failure event failure) result
runMonitor (Done r) _ = Right r
runMonitor (Fail f) _ = Left f
runMonitor (Step end _) [] = end
runMonitor (Step _ continue) (ev : rest) =
  runMonitor (continue ev) rest

data Failure event failure
  = UnexpectedEnd String -- Label to help with debugging
  | UnexpectedEvent event
  | Failed !failure
  deriving (Eq, Show)

onEnd ::
  Monitor event failure result ->
  Either (Failure event failure) result
onEnd (Done r) = Right r
onEnd (Fail f) = Left f
onEnd (Step end _) = end

-- | Applicative instance is sequential;
--
-- > liftA2 (,) p q
--
-- applies @p@, and THEN @q@.
--
-- To observe both in parallel, see 'Concurrently' or 'both'
instance Applicative (Monitor event failure) where
  pure = Done

  Done f <*> ma = f <$> ma
  Fail f <*> _ = Fail f
  Step end continue <*> ma =
    Step
      (end <*> onEnd ma)
      (\e -> continue e <*> ma)

instance Monad (Monitor event failure) where
  return = pure

  Done a >>= f = f a
  Fail f >>= _ = Fail f
  Step end continue >>= f =
    Step
      (end >>= \a -> onEnd (f a))
      (continue >=> f)

newtype Concurrently event failure result
  = Concurrently {runConcurrently :: Monitor event failure result}
  deriving (Functor)

-- | Applicative instance is concurrent;
--
-- > liftA2 (,) p q
--
-- applies @p@ and @q@ at the same time.
instance Applicative (Concurrently event failure) where
  pure = Concurrently . pure

  Concurrently p <*> Concurrently q = Concurrently (go p q)
    where
      go (Fail f) _ = Fail f
      go _ (Fail f) = Fail f
      go (Done f) x = fmap f x -- concurrence happens here
      go f (Done a) = fmap ($ a) f
      go (Step ef kf) (Step ea ka) =
        Step (ef <*> ea) (\e -> go (kf e) (ka e))

-- | Succeeds if both branches succeed
both ::
  Monitor event failure a ->
  Monitor event failure b ->
  Monitor event failure (a, b)
both p q =
  runConcurrently $
    (,)
      <$> Concurrently p
      <*> Concurrently q

expect :: Bool -> failure -> Monitor event failure ()
expect True _ = pure ()
expect False f = Fail (Failed f)

-- | Succeeds if the provided filter succeeds at any point
eventually :: (event -> Maybe a) -> Monitor event failure a
eventually f = go
  where
    go = Step (Left (UnexpectedEnd "eventually")) (maybe go Done . f)

-- | Succeeds if the provided filter never matches for the
-- rest of the sequence
never :: (event -> Maybe a) -> Monitor event failure ()
never f = go
  where
    go =
      Step
        (Right ())
        ( \ev ->
            maybe
              go
              (\_ -> Fail (UnexpectedEvent ev))
              (f ev)
        )

always :: (event -> Bool) -> Monitor event failure ()
always f = go
  where
    go =
      Step
        (Right ())
        ( \e ->
            if f e
              then go
              else
                Fail (UnexpectedEnd "always")
        )

-- | @'until' p q@ says that @q@ must eventually occur, and before it does,
-- @p@ must hold for every event.
--
-- For a weaker expectation, see 'weakUntil'.
until :: (event -> Bool) -> (event -> Maybe result) -> Monitor event failure result
until predicate f = go
  where
    go = Step (Left (UnexpectedEnd "until")) $ \ev ->
      case f ev of
        Just result -> Done result
        Nothing
          | predicate ev -> go
          | otherwise -> Fail (UnexpectedEvent ev)

-- | @'weakUntil' p q@ says that @q@ might eventually occur, and before it does,
-- @p@ must hold for every event.
--
-- For a stronger expectation, see 'until'.
weakUntil ::
  (event -> Bool) ->
  (event -> Maybe result) ->
  Monitor event failure (Maybe result)
weakUntil predicate f = go
  where
    go = Step (Right Nothing) $ \ev ->
      case f ev of
        Just result -> Done (Just result)
        Nothing
          | predicate ev -> go
          | otherwise -> Fail (UnexpectedEvent ev)

-- | 'collectUntil p q' collects the results of @p@,
-- until @q@ is satisfied.
collectUntil ::
  (event -> Maybe collect) ->
  (event -> Maybe end) ->
  Monitor event failure ([collect], end)
collectUntil collect isEnd = first reverse <$> scanUntil [] (flip (:)) collect isEnd

-- | @'scanUntil' start accumulate collect isEnd@ scans events that match @collect@
-- and accumulates then with @accumulate@ until @isEnd@ matches.
scanUntil ::
  s ->
  (s -> a -> s) ->
  (e -> Maybe a) ->
  (e -> Maybe end) ->
  Monitor e failure (s, end)
scanUntil start accumulate collect isEnd = go start
  where
    go !acc =
      Step
        (Left (UnexpectedEnd "scanUntil"))
        ( \ev -> case (isEnd ev, collect ev) of
            (Nothing, Just toCollect) -> go (accumulate acc toCollect)
            (Nothing, Nothing) -> go acc
            (Just end, _) -> Done (acc, end)
        )

-- | @'next' f@ asserts that an event satisfying @f@ must immediately
-- follow.
next :: (event -> Maybe result) -> Monitor event failure result
next f =
  Step
    (Left (UnexpectedEnd "next"))
    (\ev -> maybe (Fail (UnexpectedEvent ev)) Done (f ev))

-- | @'whenever' p q@ says that once @p@ is satisfied,
-- @q@ should hold.
whenever ::
  (event -> Maybe result) ->
  (result -> Monitor event failure ()) ->
  Monitor event failure ()
whenever trigger afterTrigger = go []
  where
    go kids = Step (traverse_ onEnd kids) $ \ev ->
      let kids' = [k' | Step _ k <- kids, k' <- [k ev]]
          kids'' = maybe kids' (\a -> afterTrigger a : kids') (trigger ev)
       in case [r | Fail r <- kids''] of
            (r : _) -> Fail r
            [] -> go [k | k@Step {} <- kids'']
