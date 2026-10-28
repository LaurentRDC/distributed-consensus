{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}

module Distributed.Consensus.Raft.Domain.Admin
  ( AdminRequestId (..),
    AdminRequest,
    AdminResponse,
    AdminCommand (..),
    AdminCommandResult (..),
  )
where

import Data.Binary (Binary)
import Distributed.Consensus.Raft.Domain (ClusterConfiguration, SequenceNum, SessionId)
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

data AdminRequestId
  = MkAdminRequestId
      SessionId
      SequenceNum
  deriving stock (Generic, Eq, Ord, Show)

instance Binary AdminRequestId

type AdminRequest node = Request AdminRequestId node (AdminCommand node)

data AdminCommandResult node
  = JoinInitiated
  | LeaveInitiated
  | ShutdownInitiated
  | ClusterConfigurationIs !(ClusterConfiguration node)
  | -- | The contacted node isn't the leader. The 'Maybe' contains the
    -- | node believed to be the current leader, if known.
    NotLeader !(Maybe node)
  deriving (Eq, Show, Generic)

instance (Binary node) => Binary (AdminCommandResult node)

type AdminResponse node = Response AdminRequestId node (AdminCommandResult node)
