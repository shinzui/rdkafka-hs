module Kafka.PropertySupport (generatedGroup, boundedList, smallText, shrinkText) where

import Data.Text (Text)
import Data.Text qualified as Text
import Test.Tasty (TestTree, localOption, testGroup)
import Test.Tasty.QuickCheck qualified as QC

-- Keep the adapter's case-count and replay options while printing replay
-- tokens for every generated suite, including successful runs.
generatedGroup :: String -> [TestTree] -> TestTree
generatedGroup name = localOption (QC.QuickCheckShowReplay True) . testGroup name

boundedList :: Int -> QC.Gen a -> QC.Gen [a]
boundedList limit element = QC.sized $ \size -> do
  count <- QC.choose (0, min limit size)
  QC.vectorOf count element

smallText :: QC.Gen Text
smallText = QC.elements ["", "shared", "other", "0", "1", "gzip", "λ", "a\0b"]

shrinkText :: Text -> [Text]
shrinkText = map Text.pack . QC.shrink . Text.unpack
