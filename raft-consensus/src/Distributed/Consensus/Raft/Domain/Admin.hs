{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Distributed.Consensus.Raft.Domain.Admin
  ( AdminRequestId (..),
    AdminRequest,
    AdminResponse,
    AdminCommand (..),
    AdminCommandResult (..),
  )
where

import Data.Binary (Binary)
import Data.Text (Text)
import Data.Word (Word64)
import Distributed.Consensus.Raft.Domain (ClusterConfiguration)
import Distributed.Consensus.Raft.Messaging (Request (..), Response (..))
import GHC.Generics (Generic)

data AdminCommand node
  = JoinCluster
      -- | Target node
      node
  | LeaveCluster
  | -- | Ask a node for the cluster configuration it has committed.
    GetClusterConfiguration
  | ShutDown
  deriving (Eq, Show, Ord, Generic)

instance (Binary node) => Binary (AdminCommand node)

newtype AdminRequestId = AdminRequestId Word64
  deriving stock (Generic, Eq, Ord, Show)
  deriving newtype (Real, Binary, Enum, Num, Integral)

type AdminRequest node = Request AdminRequestId node (AdminCommand node)

data AdminCommandResult node
  = JoinInitiated
  | LeaveInitiated
  | ShutdownInitiated
  | ClusterConfigurationIs !(ClusterConfiguration node)
  | -- | The command could not be completed.
    AdminFailure !Text
  | -- | The contacted node isn't the leader. The 'Maybe' contains the
    -- | node believed to be the current leader, if known.
    NotLeader !(Maybe node)
  deriving (Eq, Show, Generic)

instance (Binary node) => Binary (AdminCommandResult node)

type AdminResponse node = Response AdminRequestId node (AdminCommandResult node)
