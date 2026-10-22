-- These tests cover the deprecated crMapKey, crMapValue and crMapKV.
{-# OPTIONS_GHC -Wno-deprecations #-}

module Kafka.Consumer.ConsumerRecordMapSpec
  ( tests,
  )
where

import Data.Bitraversable
import Data.Text
import Kafka.Consumer.Types
import Kafka.Types
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (testCase, (@?=))

testKey, testValue :: Text
testKey = "some-key"
testValue = "some-value"

testRecord :: ConsumerRecord (Maybe Text) (Maybe Text)
testRecord =
  ConsumerRecord
    { crTopic = "some-topic",
      crPartition = PartitionId 0,
      crOffset = Offset 5,
      crTimestamp = NoTimestamp,
      crHeaders = mempty,
      crKey = Just testKey,
      crValue = Just testValue
    }

tests :: TestTree
tests =
  testGroup
    "Kafka.Consumer.ConsumerRecordMapSpec"
    [ testCase "should extract key" $
        bitraverse id pure testRecord @?= Just (crMapKey (const testKey) testRecord),
      testCase "should extract value" $
        sequence testRecord @?= Just (crMapValue (const testValue) testRecord),
      testCase "should extract both key and value" $
        bisequence testRecord @?= Just (crMapKV (const testKey) (const testValue) testRecord)
    ]
