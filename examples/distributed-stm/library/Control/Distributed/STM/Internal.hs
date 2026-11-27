{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Control.Distributed.STM.Internal
  ( -- * State machine
    txEngine,

    -- ** Types
    DVarId (..),
    Store,
    emptyStore,
    TxCommand (..),
    TxResult (..),
    Version,
    Write (..),
    WriteError (..),
    writePayload,
  )
where

import Data.Binary (Binary)
import Data.ByteString (StrictByteString)
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.Word (Word64)
import Distributed.Consensus.Raft (SessionId, StateMachineStep (Read, Transition))
import GHC.Generics (Generic)

newtype Version = Version Clock
  deriving (Eq, Ord, Show, Enum, Bounded)
  deriving newtype (Binary)

newtype Clock = MkClock Word64
  deriving (Eq, Ord, Enum, Show, Bounded, Num)
  deriving newtype (Binary)

data Cell = MkCell
  { cVersion :: !Version,
    cValue :: !StrictByteString
  }
  deriving (Show, Generic)

instance Binary Cell

-- | Uniquely identifies a DVar.
data DVarId = MkDVarId SessionId Word64
  deriving (Eq, Ord, Show, Generic)

instance Binary DVarId

data Store = MkStore
  { sClock :: !Clock,
    sVars :: !(Map DVarId Cell)
  }
  deriving (Show, Generic)

instance Binary Store

emptyStore :: Store
emptyStore = MkStore 0 mempty

data Write
  = Create StrictByteString
  | Update StrictByteString
  deriving (Show, Generic)

instance Binary Write

data WriteError
  = AlreadyExists
  | NoSuchDVar
  deriving (Show, Generic)

instance Binary WriteError

data TxCommand
  = Fetch (Map DVarId Version) DVarId -- TODO: allow to fetch multiple DVarIds at once
  | Commit (Map DVarId Version) (Map DVarId Write)
  deriving (Show, Generic)

instance Binary TxCommand

data TxResult
  = Value Version StrictByteString -- TODO: allow returning multiple values at once
  | Rejected [(DVarId, WriteError)]
  | Committed
  | Conflict [(DVarId, Maybe Cell)]
  deriving (Show, Generic)

instance Binary TxResult

txEngine :: TxCommand -> StateMachineStep TxResult Store
txEngine (Fetch readSet v) = Read $ \st ->
  case stale st readSet of
    [] -> maybe (Rejected [(v, NoSuchDVar)]) (\c -> Value (cVersion c) (cValue c)) (Map.lookup v (sVars st))
    cs -> Conflict cs
txEngine (Commit readSet writeSet) = Transition $ \st ->
  case stale st readSet of
    [] -> applyWrites (succ (sClock st)) writeSet st
    cs -> (st, Conflict cs)

stale :: Store -> Map DVarId Version -> [(DVarId, Maybe Cell)]
stale st = Map.foldrWithKey check mempty
  where
    check tid seen acc =
      case Map.lookup tid (sVars st) of
        Just cell | cVersion cell == seen -> acc
        current -> (tid, current) : acc

applyWrites :: Clock -> Map DVarId Write -> Store -> (Store, TxResult)
applyWrites now writeSet st
  | Map.null writeSet = (st, Committed)
  | not (null errs) = (st, Rejected errs)
  | otherwise = (st {sClock = now, sVars = Map.union fresh (sVars st)}, Committed)
  where
    errs = Map.foldrWithKey check [] writeSet
      where
        check tid w acc = case (w, Map.member tid (sVars st)) of
          (Create {}, True) -> (tid, AlreadyExists) : acc
          (Update {}, False) -> (tid, NoSuchDVar) : acc
          _ -> acc

    fresh = Map.map (MkCell (Version now) . writePayload) writeSet

writePayload :: Write -> StrictByteString
writePayload (Create bytes) = bytes
writePayload (Update bytes) = bytes
