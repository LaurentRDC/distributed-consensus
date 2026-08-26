{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

module Control.Monitor.Reason
  ( -- * Reason
    Reason (reasonMessage, reasonLabels, reasonIndex, reasonEvent),
    simpleReason,
    unexpectedEnd,
    unexpectedEvent,

    -- * 'Reasons'
    Reasons (..),
    oneReason,

    -- * Pretty-printers
    ppReasons,
    ppReasonsWithTrace,
  )
where

import Data.List (sortOn)
import Data.List.NonEmpty (NonEmpty)
import qualified Data.List.NonEmpty as NonEmpty
import Data.Maybe (mapMaybe)
import Data.Ord (Down (..))
import Data.Text (Text)
import qualified Data.Text as Text
import Prelude hiding (until)

data Reason event = Reason
  { reasonMessage :: !Text,
    reasonLabels :: ![Text],
    reasonIndex :: !(Maybe Int), -- Nothing = end of trace
    reasonEvent :: !(Maybe event)
  }

simpleReason :: Text -> Reason event
simpleReason msg = Reason msg [] Nothing Nothing

unexpectedEnd :: Reason event
unexpectedEnd = simpleReason "Unexpected end"

unexpectedEvent :: event -> Reason event
unexpectedEvent ev = (simpleReason "Unexpected event") {reasonEvent = Just ev}

newtype Reasons event = Reasons {unReasons :: NonEmpty (Reason event)}
  deriving (Semigroup)

oneReason :: Reason event -> Reasons event
oneReason = Reasons . NonEmpty.singleton

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

-- | Render accumulated failures (e.g. from 'anyOf'), megaparsec-style:
-- failures at the furthest position are shown in full first, on the
-- heuristic that the branch that got furthest is the one the author meant;
-- the rest are summarized one per line.
ppReasons :: (e -> Text) -> Reasons e -> Text
ppReasons ppEvent (Reasons rs) =
  case ranked of
    [r] -> ppReason ppEvent r
    (r : rest) ->
      Text.intercalate "\n" $
        [ppReason ppEvent r]
          <> ["", "other branches failed earlier:"]
          <> [indent 2 (summary r') | r' <- rest]
    [] -> error "ppReasons: impossible, NonEmpty"
  where
    -- eof failures rank furthest (the whole trace was consumed)
    rank r = maybe (Down (Just maxBound)) (Down . Just) (reasonIndex r)
    ranked = sortOn rank (NonEmpty.toList rs)
    summary Reason {..} =
      posStr <> "expected " <> reasonMessage <> ctxStr
      where
        posStr = case reasonIndex of
          Just i -> "at event " <> Text.show i <> ": "
          Nothing -> "at end of trace: "
        ctxStr = case reasonLabels of
          (c : _) -> "  (in: " <> Text.show c <> ")"
          [] -> ""

indent :: Int -> Text -> Text
indent n = (Text.replicate n " " <>)

-- | Render accumulated failures together with one shared trace window.
--
-- The furthest-position failure ("primary", per the 'ppReasons' heuristic)
-- is rendered in full. A single excerpt is shown around the primary
-- position; within it, the primary failure is gutter-marked with @>@ and
-- any other branch's failure position falling inside the window with @*@.
-- Remaining branches are then summarized, annotated with @*@ when their
-- position is visible in the excerpt above.
--
-- > scenario failed at event 4182:
-- >     BecameLeader "n2" 7
-- >   expected: no rival leader in term 7
-- >   in: leadership phase
-- >
-- > recent events:
-- >   * 4180 | VoteGranted "n3" "n2" 7
-- >     4181 | RequestVoteSent "n2" 7
-- >   > 4182 | BecameLeader "n2" 7
-- >     4183 | HeartbeatSent "n2" 7
-- >
-- > other branches failed earlier:
-- >   * at event 4180: expected vote from a quorum  (in: election phase)
-- >     at end of trace: expected heartbeat from n1  (in: leadership phase)
ppReasonsWithTrace ::
  (e -> Text) ->
  -- | radius
  Int ->
  -- | indexed trace (or excerpt)
  [(Int, e)] ->
  Reasons e ->
  Text
ppReasonsWithTrace ppEvent radius trace (Reasons rs) =
  Text.intercalate "\n" $
    [ppReason ppEvent primary]
      <> excerpt
      <> others
  where
    ranked = sortOn (Down . rankPos . reasonIndex) (NonEmpty.toList rs)
    (primary, rest) = case ranked of
      (p : r) -> (p, r)
      [] -> error "ppReasonsWithTrace: impossible, NonEmpty"

    -- eof counts as furthest: the whole trace was consumed
    rankPos = maybe (Just maxBound) Just

    focus = case reasonIndex primary of
      Just i -> i
      Nothing -> case trace of [] -> 0; _ -> fst (last trace)

    window =
      [ (i, e)
      | (i, e) <- trace,
        i >= focus - radius,
        i <= focus + radius
      ]

    restPositions = mapMaybe reasonIndex rest
    visible i = any (\(j, _) -> j == i) window

    excerpt = case window of
      [] -> []
      _ -> ["", "recent events:"] <> map row window
      where
        width = Text.length (Text.show (maximum (map fst window)))
        row (i, e) = gutter <> pad width (Text.show i) <> " | " <> ppEvent e
          where
            gutter
              | Just i == reasonIndex primary = "  > "
              | i `elem` restPositions = "  * "
              | otherwise = "    "

    others = case rest of
      [] -> []
      _ -> ["", "other branches failed earlier:"] <> map line rest
      where
        line r = "  " <> mark <> summaryLine r
          where
            mark = case reasonIndex r of
              Just i | visible i -> "* "
              _ -> "  "

    summaryLine Reason {..} =
      posStr <> "expected " <> reasonMessage <> ctxStr
      where
        posStr = case reasonIndex of
          Just i -> "at event " <> Text.show i <> ": "
          Nothing -> "at end of trace: "
        ctxStr = case reasonLabels of
          (c : _) -> "  (in: " <> Text.show c <> ")"
          [] -> ""

pad :: Int -> Text -> Text
pad w s = Text.replicate (w - Text.length s) " " <> s
