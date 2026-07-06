{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

module Control.Monitor
  ( -- * Types
    Monitor,
    Concurrently,
    Reason (reasonMessage, reasonLabels, reasonIndex, reasonEvent),
    ppReason,
    ppReasonWithTrace,

    -- ** Running the 'Monitor'
    runMonitor,

    -- ** Defining 'Monitor's
    label,
    (<?>),

    -- * Combinators
    assert,
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
  )
where

import Control.Applicative ((<|>))
import Control.Monad (unless, (>=>))
import Data.Bifunctor (first)
import Data.Foldable (traverse_)
import Data.Text (Text)
import qualified Data.Text as Text
import Prelude hiding (until)

data Monitor event result
  = Done !result
  | Fail !(Reason event)
  | Step
      -- | What to do on end-of-input
      (Either (Reason event) result)
      -- | How to continue
      (event -> Monitor event result)
  deriving (Functor)

data Failure event failure
  = UnexpectedEnd String -- Label to help with debugging
  | UnexpectedEvent event
  | Failed !failure
  deriving (Eq, Show)

data Reason event = Reason
  { reasonMessage :: !Text,
    reasonLabels :: ![Text],
    reasonIndex :: !(Maybe Int), -- Nothing = end of trace
    reasonEvent :: !(Maybe event)
  }

-- | Render a single failure.
--
-- > scenario failed at event 4182:
-- >     BecameLeader n2 (Term 7)
-- >   expected: no rival leader in term 7
-- >   in: leadership phase
-- >   in: scenario "uncontested election"
ppReason :: (e -> Text) -> Reason e -> Text
ppReason ppEvent Reason {..} =
  Text.intercalate "\n" $
    [header]
      <> [indent 4 (ppEvent e) | Just e <- [reasonEvent]]
      <> [indent 2 ("expected: " <> reasonMessage)]
      <> [indent 2 ("in: " <> c) | c <- reasonLabels]
  where
    header = case reasonIndex of
      Just i -> "scenario failed at event " <> Text.show i <> ":"
      Nothing -> "scenario failed at end of trace:"

-- -- | Render accumulated failures (e.g. from 'anyOf'), megaparsec-style:
-- -- failures at the furthest position are shown in full first, on the
-- -- heuristic that the branch that got furthest is the one the author meant;
-- -- the rest are summarized one per line.
-- ppReasons :: (e -> String) -> Reasons e -> String
-- ppReasons ppEvent (Reasons rs) =
--   case ranked of
--     [r] -> ppReason ppEvent r
--     (r : rest) ->
--       intercalate "\n" $
--         [ppReason ppEvent r]
--           ++ ["", "other branches failed earlier:"]
--           ++ [indent 2 (summary r') | r' <- rest]
--     [] -> error "ppReasons: impossible, NonEmpty"
--   where
--     -- eof failures rank furthest (the whole trace was consumed)
--     rank r = maybe (Down (Just maxBound)) (Down . Just) (reasonPos r)
--     ranked = sortOn rank (NE.toList rs)
--     summary Reason {..} =
--       posStr ++ "expected " ++ reasonMessage ++ ctxStr
--       where
--         posStr = case reasonPos of
--           Just i -> "at event " ++ show i ++ ": "
--           Nothing -> "at end of trace: "
--         ctxStr = case reasonContext of
--           (c : _) -> "  (in: " ++ c ++ ")"
--           [] -> ""

-- | Render a failure together with a window of the trace around the
-- offending event, gutter-marked like a source excerpt.
--
-- The full trace (or any suffix-aligned excerpt: pass the events paired
-- with their true indices) is supplied; @radius@ events are shown on
-- each side of the failure position.
--
-- > recent events:
-- >   4180 | ElectionTimeout n2 (Term 7)
-- >   4181 | RequestVoteSent n2 (Term 7)
-- > > 4182 | BecameLeader n2 (Term 7)
-- >   4183 | HeartbeatSent n2 (Term 7)
ppReasonWithTrace ::
  (e -> Text) ->
  -- | radius
  Int ->
  -- | indexed trace (or excerpt)
  [(Int, e)] ->
  Reason e ->
  Text
ppReasonWithTrace ppEvent radius trace r =
  ppReason ppEvent r <> case window of
    [] -> ""
    _ -> "\n\nrecent events:\n" <> Text.intercalate "\n" (map row window)
  where
    focus = case reasonIndex r of
      Just i -> i
      Nothing -> case trace of [] -> 0; _ -> fst (last trace)
    window =
      [ (i, e)
      | (i, e) <- trace,
        i >= focus - radius,
        i <= focus + radius
      ]
    width = case window of
      [] -> 1
      _ -> Text.length (Text.show (maximum (map fst window)))
    row (i, e) = gutter <> pad (Text.show i) <> " | " <> ppEvent e
      where
        gutter = if Just i == reasonIndex r then "> " else "  "
    pad s = Text.replicate (width - Text.length s) " " <> s

indent :: Int -> Text -> Text
indent n = (Text.replicate n " " <>)

simpleReason :: Text -> Reason event
simpleReason msg = Reason msg [] Nothing Nothing

unexpectedEnd :: Reason event
unexpectedEnd = simpleReason "Unexpected end"

unexpectedEvent :: event -> Reason event
unexpectedEvent ev = (simpleReason "Unexpected event") {reasonEvent = Just ev}

label :: Text -> Monitor event result -> Monitor event result
label l = mapReason (\r -> r {reasonLabels = l : reasonLabels r})

infix 0 <?>

(<?>) :: Monitor event a -> Text -> Monitor event a
(<?>) = flip label

mapReason :: (Reason event -> Reason event) -> Monitor event result -> Monitor event result
mapReason _ (Done a) = Done a
mapReason f (Fail r) = Fail (f r)
mapReason f (Step end continue) = Step (first f end) (mapReason f . continue)

runMonitor ::
  Monitor event result ->
  [event] ->
  Either (Reason event) result
runMonitor = go 0
  where
    go !_ (Done a) _ = Right a
    go !ix (Fail r) _ = Left (markPosition ix Nothing r)
    go !ix (Step end _) [] = either (Left . markPosition ix Nothing) Right end
    go !ix (Step _ continue) (ev : rest) = case continue ev of
      Fail r -> Left (markPosition ix (Just ev) r)
      m -> go (succ ix) m rest

    markPosition ix mEvent r =
      r
        { reasonIndex = reasonIndex r <|> Just ix,
          reasonEvent = reasonEvent r <|> mEvent
        }

onEnd ::
  Monitor event result ->
  Either (Reason event) result
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
instance Applicative (Monitor event) where
  pure = Done

  Done f <*> ma = f <$> ma
  Fail f <*> _ = Fail f
  Step end continue <*> ma =
    Step
      (end <*> onEnd ma)
      (\e -> continue e <*> ma)

instance Monad (Monitor event) where
  return = pure

  Done a >>= f = f a
  Fail f >>= _ = Fail f
  Step end continue >>= f =
    Step
      (end >>= \a -> onEnd (f a))
      (continue >=> f)

newtype Concurrently event failure result
  = Concurrently {runConcurrently :: Monitor event result}
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
  Monitor event a ->
  Monitor event b ->
  Monitor event (a, b)
both p q =
  runConcurrently $
    (,)
      <$> Concurrently p
      <*> Concurrently q

assert :: Text -> Bool -> Monitor event ()
assert msg cond = unless cond (Fail (simpleReason msg))

-- | Succeeds if the provided filter succeeds at any point
eventually :: (event -> Maybe a) -> Monitor event a
eventually f = go
  where
    go = Step (Left unexpectedEnd) (maybe go Done . f)

-- | Succeeds if the provided filter never matches for the
-- rest of the sequence
never :: (event -> Maybe a) -> Monitor event ()
never f = go
  where
    go =
      Step
        (Right ())
        ( \ev ->
            maybe
              go
              (\_ -> Fail (unexpectedEvent ev))
              (f ev)
        )

always :: (event -> Bool) -> Monitor event ()
always f = go
  where
    go =
      Step
        (Right ())
        ( \e ->
            if f e
              then go
              else
                Fail unexpectedEnd
        )

-- | @'until' p q@ says that @q@ must eventually occur, and before it does,
-- @p@ must hold for every event.
--
-- For a weaker expectation, see 'weakUntil'.
until :: (event -> Bool) -> (event -> Maybe result) -> Monitor event result
until predicate f = go
  where
    go = Step (Left unexpectedEnd) $ \ev ->
      case f ev of
        Just result -> Done result
        Nothing
          | predicate ev -> go
          | otherwise -> Fail (unexpectedEvent ev)

-- | @'weakUntil' p q@ says that @q@ might eventually occur, and before it does,
-- @p@ must hold for every event.
--
-- For a stronger expectation, see 'until'.
weakUntil ::
  (event -> Bool) ->
  (event -> Maybe result) ->
  Monitor event (Maybe result)
weakUntil predicate f = go
  where
    go = Step (Right Nothing) $ \ev ->
      case f ev of
        Just result -> Done (Just result)
        Nothing
          | predicate ev -> go
          | otherwise -> Fail (unexpectedEvent ev)

-- | 'collectUntil p q' collects the results of @p@,
-- until @q@ is satisfied.
collectUntil ::
  (event -> Maybe collect) ->
  (event -> Maybe end) ->
  Monitor event ([collect], end)
collectUntil collect isEnd = first reverse <$> scanUntil [] (flip (:)) collect isEnd

-- | @'scanUntil' start accumulate collect isEnd@ scans events that match @collect@
-- and accumulates then with @accumulate@ until @isEnd@ matches.
scanUntil ::
  s ->
  (s -> a -> s) ->
  (e -> Maybe a) ->
  (e -> Maybe end) ->
  Monitor e (s, end)
scanUntil start accumulate collect isEnd = go start
  where
    go !acc =
      Step
        (Left unexpectedEnd)
        ( \ev -> case (isEnd ev, collect ev) of
            (Nothing, Just toCollect) -> go (accumulate acc toCollect)
            (Nothing, Nothing) -> go acc
            (Just end, _) -> Done (acc, end)
        )

-- | @'next' f@ asserts that an event satisfying @f@ must immediately
-- follow.
next :: (event -> Maybe result) -> Monitor event result
next f =
  Step
    (Left unexpectedEnd)
    (\ev -> maybe (Fail (unexpectedEvent ev)) Done (f ev))

-- | @'whenever' p q@ says that once @p@ is satisfied,
-- @q@ should hold.
whenever ::
  (event -> Maybe result) ->
  (result -> Monitor event ()) ->
  Monitor event ()
whenever trigger afterTrigger = go []
  where
    go kids = Step (traverse_ onEnd kids) $ \ev ->
      let kids' = [k' | Step _ k <- kids, k' <- [k ev]]
          kids'' = maybe kids' (\a -> afterTrigger a : kids') (trigger ev)
       in case [r | Fail r <- kids''] of
            (r : _) -> Fail r
            [] -> go [k | k@Step {} <- kids'']
