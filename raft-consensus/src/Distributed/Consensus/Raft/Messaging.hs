{-# LANGUAGE DeriveGeneric #-}

module Distributed.Consensus.Raft.Messaging
  ( Request (..),
    Response (..),
  )
where

import Data.Binary (Binary)
import GHC.Generics (Generic)

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
