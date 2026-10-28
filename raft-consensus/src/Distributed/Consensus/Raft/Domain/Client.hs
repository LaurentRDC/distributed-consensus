{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}

module Distributed.Consensus.Raft.Domain.Client
  ( SessionId,
    sessionId,
    SequenceNum,
    ClientRequestId (..),
    ClientRequest,
    ClientResponse,
    ClientResult (..),
  )
where

import Data.Binary (Binary)
import Distributed.Consensus.Raft.Domain (SequenceNum, SessionId, sessionId)
import Distributed.Consensus.Raft.Messaging (Request (..), Response (..))
import GHC.Generics (Generic)

-- | Client-sourced request ID. This allows to correlate multiple client
-- side-responses.
data ClientRequestId
  = MkClientRequestId
      SessionId
      SequenceNum
  deriving stock (Generic, Eq, Ord, Show)

instance Binary ClientRequestId

type ClientRequest node entry = Request ClientRequestId node entry

data ClientResult result
  = Success !result
  | NotLeader
  deriving (Eq, Show, Generic)

instance (Binary result) => Binary (ClientResult result)

type ClientResponse node result = Response ClientRequestId node (ClientResult result)
