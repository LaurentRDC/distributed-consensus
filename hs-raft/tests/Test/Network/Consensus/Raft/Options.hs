module Test.Network.Consensus.Raft.Options
  ( setNumRacyTests,
    withExplorationOptions,
    withPrintTraceOption,
    NumRacyTests,
    ScheduleBound,
    BranchingFactor,
    PrintTrace (..),
  )
where

import Control.Monad.IOSim (ExplorationOptions, withBranching, withScheduleBound)
import Data.Maybe (fromMaybe)
import Data.Monoid (Endo (..))
import Data.Tagged (Tagged (..))
import Test.Tasty (TestTree, askOption, localOption)
import Test.Tasty.Options
  ( IsOption (..),
    mkFlagCLParser,
    safeRead,
    safeReadBool,
  )
import Test.Tasty.QuickCheck
  ( QuickCheckTests (QuickCheckTests),
  )

setNumRacyTests :: TestTree -> TestTree
setNumRacyTests tree =
  -- These tests are potentially very long. We want a small default (here, 3),
  -- but with the ability to set it to a larger or smaller number at weill.
  --
  -- 'QuickCheckTests' doesn't allow this, as its default is 100, which is much
  -- too large
  askOption $ \(NumRacyTests n) ->
    let numTests = fromMaybe 3 n
     in localOption (QuickCheckTests numTests) tree

newtype NumRacyTests
  = NumRacyTests (Maybe Int)
  deriving (Eq, Ord, Show)

withExplorationOptions :: ((ExplorationOptions -> ExplorationOptions) -> TestTree) -> TestTree
withExplorationOptions f =
  askOption $ \(ScheduleBound mBound) ->
    askOption $ \(BranchingFactor mFactor) ->
      f
        ( appEndo $
            mconcat
              [ maybe mempty (Endo . withScheduleBound) mBound,
                maybe mempty (Endo . withBranching) mFactor
              ]
        )

instance IsOption NumRacyTests where
  defaultValue = NumRacyTests Nothing
  parseValue s = NumRacyTests . Just <$> safeRead s
  optionName = Tagged "num-racy-tests"
  optionHelp = Tagged "Number of racy tests to run"

newtype ScheduleBound
  = ScheduleBound (Maybe Int)
  deriving (Eq, Ord, Show)

instance IsOption ScheduleBound where
  defaultValue = ScheduleBound Nothing
  parseValue s = ScheduleBound . Just <$> safeRead s
  optionName = Tagged "schedule-bound"
  optionHelp = Tagged "Upper bound on the number of schedules with race reversals that will be explored."

newtype BranchingFactor
  = BranchingFactor (Maybe Int)
  deriving (Eq, Ord, Show)

instance IsOption BranchingFactor where
  defaultValue = BranchingFactor Nothing
  parseValue s = BranchingFactor . Just <$> safeRead s
  optionName = Tagged "branching-factor"
  optionHelp = Tagged "Number of alternative schedules explored per race reversal."

withPrintTraceOption :: (PrintTrace -> TestTree) -> TestTree
withPrintTraceOption = askOption

newtype PrintTrace
  = PrintTrace Bool
  deriving (Eq, Ord, Show)

instance IsOption PrintTrace where
  defaultValue = PrintTrace False
  optionName = Tagged "print-trace"
  parseValue = fmap PrintTrace . safeReadBool
  optionHelp = Tagged "Print the execution trace. This is generally only useful for a specific test replay."
  optionCLParser = mkFlagCLParser mempty (PrintTrace True)
