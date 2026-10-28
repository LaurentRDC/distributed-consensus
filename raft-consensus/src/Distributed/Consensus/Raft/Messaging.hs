{-# LANGUAGE DeriveGeneric #-}

module Distributed.Consensus.Raft.Messaging
  ( Request (..),
    Response (..),

    -- * Payloads
    Payload (New, Retry),
    newPayload,
    retryPayload,
    extractPayload,
  )
where

import Data.Binary (Binary)
import GHC.Generics (Generic)

-- | A 'Payload' allows a function to determine
-- whether a piece of data is new ('New'), or whether
-- this is a payload that's being retried ('Retry')
data Payload id a
  = -- | New payload; needs to have an 'id' generated
    New a
  | -- | Retried payload should re-use the 'id'
    Retry id a
  deriving (Eq, Show, Ord)

newPayload :: a -> Payload id a
newPayload = New

retryPayload :: id -> a -> Payload id a
retryPayload = Retry

extractPayload :: Payload id a -> a
extractPayload (New x) = x
extractPayload (Retry _ x) = x

-- | Request incoming to the Raft cluster.
--
-- This is an envelope that collects things that any request
-- needs to be processed end-to-end.
data Request id node payload
  = MkRequest
  { requestId :: !id,
    requestOriginator :: !node,
    requestPayload :: !payload
  }
  deriving (Eq, Show, Generic)

instance (Binary id, Binary node, Binary payload) => Binary (Request id node payload)

data Response id node payload
  = MkResponse
  { responseRequestId :: !id,
    responseClusterLeader :: !(Maybe node),
    responsePayload :: !payload
  }
  deriving (Eq, Show, Generic)

instance (Binary id, Binary node, Binary payload) => Binary (Response id node payload)
