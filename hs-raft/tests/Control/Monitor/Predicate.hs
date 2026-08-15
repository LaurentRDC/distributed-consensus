{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Control.Monitor.Predicate
  ( Predicate (runPredicate),
    predicate,
    contramap,

    -- ** Combining predicates
    and,
    or,
  )
where

import Control.Applicative (Alternative (..))
import Prelude hiding (and, or)

-- | A 'Predicate' is a function which either fails,
-- or succeeds by returning data.
--
-- This is a generalization of functions of type @a -> 'Bool'@.
--
-- Use 'fmap' to map the result of a 'Predicate', and 'contramap'
-- to map over the input of 'Predicate'.
--
-- Use 'predicate' to turn a regular function into a 'Predicate'.
-- Use 'and' or 'or' to compose 'Predicate's together.
newtype Predicate e a = Predicate {runPredicate :: e -> Maybe a}
  deriving (Functor)

predicate :: (e -> Maybe a) -> Predicate e a
predicate = Predicate

contramap :: (e -> e') -> Predicate e' a -> Predicate e a
contramap f (Predicate g) = Predicate (g . f)

instance Applicative (Predicate e) where
  pure a = predicate (const (Just a))

  Predicate f <*> Predicate x =
    Predicate $ \e -> case (f e, x e) of
      (Just g, Just a) -> Just $ g a
      _ -> Nothing

instance Alternative (Predicate e) where
  empty = predicate (const Nothing)

  Predicate f <|> Predicate g = Predicate $
    \e -> f e <|> g e

instance Monad (Predicate e) where
  return = pure

  Predicate x >>= f = Predicate $ \e -> (\a -> runPredicate (f a) e) =<< x e

instance MonadFail (Predicate e) where
  fail _ = Predicate (const Nothing)

and :: Predicate e a -> Predicate e b -> Predicate e (a, b)
and = liftA2 (,)

or :: Predicate e a -> Predicate e b -> Predicate e (Either a b)
or (Predicate left) (Predicate right) =
  Predicate $ \e -> case (left e, right e) of
    (Just l, _) -> pure $ Left l
    (_, Just r) -> pure $ Right r
    _ -> Nothing
