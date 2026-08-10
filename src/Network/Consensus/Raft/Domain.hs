{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Network.Consensus.Raft.Domain
  ( Term,
    RequestId,
    Role (..),
  )
where

import Data.Int (Int64)
import GHC.Generics (Generic)

-- | Election term
newtype Term = Term Int64
  deriving stock (Generic, Eq, Ord, Show)
  deriving newtype (Real, Enum, Num, Integral)

-- | A 'RequestId' dentifies a request from a client,
-- uniquely for each leader.
newtype RequestId = RequestId Int64
  deriving stock (Generic, Eq, Ord, Show)
  deriving newtype (Real, Enum, Num, Integral)

data Role
  = Leader
  | Follower
  | Candidate
  deriving (Eq, Show, Ord, Enum, Bounded)
