module Kafka.Consumer.ConsumerRecordTraverseSpec
  ( tests,
  )
where

import Data.Bifunctor
import Data.Bitraversable
import Data.Text
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (testCase, (@?=))
import "rdkafka-hs" Kafka.Consumer.Types
import "rdkafka-hs" Kafka.Types

testKey, testValue :: Text
testKey = "some-key"
testValue = "some-value"

testRecord :: ConsumerRecord Text Text
testRecord =
  ConsumerRecord
    { crTopic = "some-topic",
      crPartition = PartitionId 0,
      crOffset = Offset 5,
      crTimestamp = NoTimestamp,
      crHeaders = mempty,
      crKey = testKey,
      crValue = testValue
    }

liftValue :: a -> Maybe a
liftValue = Just

liftNothing :: a -> Maybe a
liftNothing _ = Nothing

tests :: TestTree
tests =
  testGroup
    "Kafka.Consumer.ConsumerRecordTraverseSpec"
    [ testCase "should sequence" $ do
        sequence (liftValue <$> testRecord) @?= Just testRecord
        sequenceA (liftValue <$> testRecord) @?= Just testRecord
        sequence (liftNothing <$> testRecord) @?= Nothing,
      testCase "should traverse" $ do
        traverse liftValue testRecord @?= Just testRecord
        traverse liftNothing testRecord @?= Nothing,
      testCase "should bisequence" $ do
        bisequence (bimap liftValue liftValue testRecord) @?= Just testRecord
        bisequence (bimap liftNothing liftValue testRecord) @?= Nothing
        bisequence (bimap liftValue liftNothing testRecord) @?= Nothing
        bisequenceA (bimap liftValue liftValue testRecord) @?= Just testRecord
        bisequenceA (bimap liftNothing liftValue testRecord) @?= Nothing
        bisequenceA (bimap liftValue liftNothing testRecord) @?= Nothing,
      testCase "should bitraverse" $ do
        bitraverse liftValue liftValue testRecord @?= Just testRecord
        bitraverse liftNothing liftValue testRecord @?= Nothing
        bitraverse liftValue liftNothing testRecord @?= Nothing
    ]
