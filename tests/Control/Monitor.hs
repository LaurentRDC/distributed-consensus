{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE OverloadedStrings #-}

module Control.Monitor
  ( -- * Types
    Monitor,
    Concurrently,
    ppReasons,
    ppReasonsWithTrace,

    -- ** Running the 'Monitor'
    runMonitor,

    -- ** Defining 'Monitor's
    label,
    (<?>),

    -- * Predicates
    module Control.Monitor.Predicate,

    -- * Algebra

    -- ** Unary operators
    next,
    eventually,
    always,

    -- ** Binary operators
    until,
    weakUntil,
    release,

    -- * Combinators
    assert,
    never,
    collectUntil,
    scanUntil,
    whenever,

    -- * Concurrent combinators
    both,
    race,
    anyOf,
    allOf,
  )
where

import Control.Applicative (Alternative (..), asum, (<|>))
import Control.Monad (unless, (>=>))
import Control.Monitor.Predicate
import Control.Monitor.Reason
  ( Reason (..),
    Reasons (..),
    oneReason,
    ppReasons,
    ppReasonsWithTrace,
    simpleReason,
    unexpectedEnd,
    unexpectedEvent,
  )
import Data.Bifunctor (first)
import Data.Foldable (traverse_)
import Data.Maybe (isJust)
import Data.Text (Text)
import qualified Data.Text as Text
import Prelude hiding (until)

data Monitor e a
  = Done !a
  | Fail !(Reasons e)
  | Step
      -- | What to do on end-of-input
      (Either (Reasons e) a)
      -- | How to continue
      (e -> Monitor e a)
  deriving (Functor)

nonEmptyStep ::
  (e -> Monitor e a) ->
  Monitor e a
nonEmptyStep = Step (Left (oneReason unexpectedEnd))

label :: Text -> Monitor e a -> Monitor e a
label l = mapReason (\r -> r {reasonLabels = l : reasonLabels r})

infix 0 <?>

(<?>) :: Monitor e a -> Text -> Monitor e a
(<?>) = flip label

mapReason :: (Reason e -> Reason e) -> Monitor e a -> Monitor e a
mapReason _ (Done a) = Done a
mapReason f (Fail (Reasons rs)) = Fail (Reasons $ fmap f rs)
mapReason f (Step end continue) = Step (first (Reasons . fmap f . unReasons) end) (mapReason f . continue)

mapReasons :: (Reasons e -> Reasons e) -> Monitor e a -> Monitor e a
mapReasons _ (Done a) = Done a
mapReasons f (Fail rs) = Fail (f rs)
mapReasons f (Step end k) = Step (first f end) (mapReasons f . k)

runMonitor ::
  Monitor e a ->
  [e] ->
  Either (Reasons e) a
runMonitor = go 0
  where
    go !_ (Done a) _ = Right a
    go !ix (Fail r) _ = Left (markPosition (Just ix) Nothing r)
    go !_ (Step end _) [] = either (Left . markPosition Nothing Nothing) Right end
    go !ix (Step _ continue) (ev : rest) = case continue ev of
      Fail r -> Left (markPosition (Just ix) (Just ev) r)
      m -> go (succ ix) m rest

    markPosition mix mEvent = Reasons . fmap markOneReason . unReasons
      where
        markOneReason r =
          r
            { reasonIndex = reasonIndex r <|> mix,
              reasonEvent = reasonEvent r <|> mEvent
            }

onEnd :: Monitor e a -> Either (Reasons e) a
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
instance Applicative (Monitor e) where
  pure = Done

  Done f <*> ma = f <$> ma
  Fail f <*> _ = Fail f
  Step end continue <*> ma =
    Step
      (end <*> onEnd ma)
      (\e -> continue e <*> ma)

instance Monad (Monitor e) where
  return = pure

  Done a >>= f = f a
  Fail f >>= _ = Fail f
  Step end continue >>= f =
    Step
      (end >>= \a -> onEnd (f a))
      (continue >=> f)

instance MonadFail (Monitor e) where
  fail = Fail . oneReason . simpleReason . Text.pack

assert :: Text -> Bool -> Monitor e ()
assert msg cond = unless cond (Fail (oneReason $ simpleReason msg))

-- | Succeeds if the provided filter succeeds at any point
eventually :: Predicate e a -> Monitor e a
eventually f = go
  where
    go = nonEmptyStep (maybe go Done . runPredicate f)

-- | Succeeds if the provided filter never matches for the
-- rest of the sequence
never :: Predicate e a -> Monitor e ()
never f = go
  where
    go =
      Step
        (Right ())
        ( \ev ->
            maybe
              go
              (\_ -> Fail (oneReason $ unexpectedEvent ev))
              (runPredicate f ev)
        )

always :: Predicate e a -> Monitor e ()
always f = go
  where
    go =
      Step
        (Right ())
        (\e -> if isJust (runPredicate f e) then go else Fail $ oneReason unexpectedEnd)

-- | @'until' p q@ says that @q@ must eventually occur, and before it does,
-- @p@ must hold for every e.
--
-- For a weaker expectation, see 'weakUntil'.
until :: (e -> Maybe b) -> Predicate e a -> Monitor e a
until pred' f = go
  where
    go = nonEmptyStep $ \ev ->
      case runPredicate f ev of
        Just result -> Done result
        Nothing
          | isJust (pred' ev) -> go
          | otherwise -> Fail (oneReason $ unexpectedEvent ev)

-- | @'weakUntil' p q@ says that @q@ might eventually occur, and before it does,
-- @p@ must hold for every e.
--
-- For a stronger expectation, see 'until'.
weakUntil ::
  (e -> Maybe b) ->
  Predicate e a ->
  Monitor e (Maybe a)
weakUntil pred' f = go
  where
    go = Step (Right Nothing) $ \ev ->
      case runPredicate f ev of
        Just result -> Done (Just result)
        Nothing
          | isJust (pred' ev) -> go
          | otherwise -> Fail (oneReason $ unexpectedEvent ev)

-- | 'collectUntil p q' collects the results of @p@,
-- until @q@ is satisfied.
collectUntil ::
  Predicate e collect ->
  Predicate e end ->
  Monitor e ([collect], end)
collectUntil collect isEnd =
  first reverse <$> scanUntil [] (flip (:)) collect isEnd

-- | @'scanUntil' start accumulate collect isEnd@ scans events that match @collect@
-- and accumulates then with @accumulate@ until @isEnd@ matches.
scanUntil ::
  s ->
  (s -> a -> s) ->
  Predicate e a ->
  Predicate e end ->
  Monitor e (s, end)
scanUntil start accumulate collect isEnd = go start
  where
    go !acc =
      nonEmptyStep $ \ev -> case (runPredicate isEnd ev, runPredicate collect ev) of
        (Nothing, Just toCollect) -> go (accumulate acc toCollect)
        (Nothing, Nothing) -> go acc
        (Just end, _) -> Done (acc, end)

-- | @'next' f@ asserts that an e satisfying @f@ must immediately
-- follow.
next :: Predicate e a -> Monitor e a
next f =
  Step
    (Left $ oneReason unexpectedEnd)
    (\ev -> maybe (Fail (oneReason $ unexpectedEvent ev)) Done (runPredicate f ev))

-- | @'release' p q@ asserts that @p@ must hold until @q@ does,
-- and then @q@ holds forever.
release ::
  Predicate e a ->
  Predicate e a ->
  Monitor e ()
release pred' f = go
  where
    go = nonEmptyStep $ \ev ->
      case (runPredicate pred' ev, runPredicate f ev) of
        (Just _, Just _) -> always f
        (Just _, Nothing) -> go
        (Nothing, _) -> Fail (oneReason $ unexpectedEvent ev)

-- | @'whenever' p q@ says that once @p@ is satisfied,
-- @q@ should hold.
whenever ::
  Predicate e a ->
  (a -> Monitor e ()) ->
  Monitor e ()
whenever trigger afterTrigger = go []
  where
    go kids = Step (traverse_ onEnd kids) $ \ev ->
      let kids' = [k' | Step _ k <- kids, k' <- [k ev]]
          kids'' = maybe kids' (\a -> afterTrigger a : kids') (runPredicate trigger ev)
       in case [r | Fail r <- kids''] of
            (r : _) -> Fail r
            [] -> go [k | k@Step {} <- kids'']

newtype Concurrently e a
  = Concurrently {runConcurrently :: Monitor e a}
  deriving (Functor)

-- | Applicative instance is concurrent;
--
-- > liftA2 (,) p q
--
-- applies @p@ and @q@ at the same time.
instance Applicative (Concurrently e) where
  pure = Concurrently . pure

  Concurrently p <*> Concurrently q = Concurrently (go p q)
    where
      go (Fail f) (Fail g) = Fail (f <> g)
      go (Fail f) _ = Fail f
      go _ (Fail g) = Fail g
      go (Done f) x = fmap f x -- concurrence happens here
      go f (Done a) = fmap ($ a) f
      go (Step ef kf) (Step ea ka) =
        Step (ef <*> ea) (\e -> go (kf e) (ka e))

-- | Succeeds if both branches succeed
both ::
  Monitor e a ->
  Monitor e b ->
  Monitor e (a, b)
both p q =
  runConcurrently $
    (,)
      <$> Concurrently p
      <*> Concurrently q

race ::
  Monitor e a ->
  Monitor e b ->
  Monitor e (Either a b)
race = go
  where
    go (Done a) _ = Done (Left a)
    go _ (Done b) = Done (Right b)
    -- one arm died: the race degrades to the surviving arm, but we
    -- remember why the loser died, in case the survivor also fails
    go ma (Fail rb) = Left <$> mapReasons (rb <>) ma
    go (Fail ra) mb = Right <$> mapReasons (<> ra) mb
    -- both still running: synchronous product step
    go (Step ea ka) (Step eb kb) =
      Step (raceEnd ea eb) (\e -> go (ka e) (kb e))

    -- eof verdict: leftmost acceptance wins; both rejecting accumulates
    raceEnd (Right a) _ = Right (Left a)
    raceEnd _ (Right b) = Right (Right b)
    raceEnd (Left ra) (Left rb) = Left (ra <> rb)

instance Alternative (Concurrently e) where
  empty = Concurrently (Fail (oneReason (simpleReason "empty")))
  Concurrently a <|> Concurrently b = Concurrently (either id id <$> race a b)

-- | Run multiple monitors and succeed if any succeeds.
-- The result is left-biased.
anyOf ::
  [Monitor e a] ->
  Monitor e a
anyOf = runConcurrently . asum . fmap Concurrently

-- | Run multiple monitors and succeed only if
-- all of the monitors succeed. Returns their results.
allOf :: [Monitor e a] -> Monitor e [a]
allOf = runConcurrently . traverse Concurrently
