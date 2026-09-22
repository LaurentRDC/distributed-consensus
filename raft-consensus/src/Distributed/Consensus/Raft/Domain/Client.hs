{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Distributed.Consensus.Raft.Domain.Client
  ( ClientRequestId (..),
    ClientRequest,
    ClientResponse,
    ClientResult (..),
  )
where

import Data.Binary (Binary)
import Data.Text (Text)
import Data.Word (Word64)
import Distributed.Consensus.Raft.Messaging (Request (..), Response (..))
import GHC.Generics (Generic)

-- Alphabet for communicating with clients

-- | Client-sourced request ID. This allows to correlate multiple client
-- side-responses.
newtype ClientRequestId = ClientRequestId Word64
  deriving stock (Generic, Eq, Ord, Show)
  deriving newtype (Real, Binary, Enum, Num, Integral)

type ClientRequest node entry = Request ClientRequestId node entry

data ClientResult result
  = Success !result
  | Failure !Text
  | NotLeader
  deriving (Eq, Show, Generic)

instance (Binary result) => Binary (ClientResult result)

type ClientResponse node result = Response ClientRequestId node (ClientResult result)
