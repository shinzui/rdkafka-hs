module Main (main) where

import Kafka.Consumer.ConsumerRecordMapSpec qualified as ConsumerRecordMapSpec
import Kafka.Consumer.ConsumerRecordTraverseSpec qualified as ConsumerRecordTraverseSpec
import Test.Tasty (defaultMain, testGroup)

main :: IO ()
main =
  defaultMain $
    testGroup
      "rdkafka-hs"
      [ ConsumerRecordMapSpec.tests,
        ConsumerRecordTraverseSpec.tests
      ]
