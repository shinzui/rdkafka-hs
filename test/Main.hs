module Main (main) where

import Kafka.Consumer.ConsumerPropertiesSpec qualified as ConsumerPropertiesSpec
import Kafka.Consumer.ConsumerRecordMapSpec qualified as ConsumerRecordMapSpec
import Kafka.Consumer.ConsumerRecordTraverseSpec qualified as ConsumerRecordTraverseSpec
import Kafka.Internal.CallbackOwnerSpec qualified as CallbackOwnerSpec
import Kafka.Producer.ProducerPropertiesSpec qualified as ProducerPropertiesSpec
import Kafka.PublicDataContractSpec qualified as PublicDataContractSpec
import Kafka.PublicPropertiesSpec qualified as PublicPropertiesSpec
import Kafka.VersionTest qualified as VersionTest
import System.Environment (getArgs)
import Test.Tasty (defaultMain, testGroup)
import "rdkafka-hs" Kafka.Version (librdkafkaVersion, librdkafkaVersionNumber)

main :: IO ()
main = do
  args <- getArgs
  case args of
    ["--native-version"] -> print (librdkafkaVersion, librdkafkaVersionNumber)
    _ -> runTests

runTests :: IO ()
runTests =
  defaultMain $
    testGroup
      "rdkafka-hs"
      [ CallbackOwnerSpec.tests,
        ConsumerPropertiesSpec.tests,
        ConsumerRecordMapSpec.tests,
        ConsumerRecordTraverseSpec.tests,
        VersionTest.tests,
        ProducerPropertiesSpec.tests,
        PublicPropertiesSpec.tests,
        PublicDataContractSpec.tests
      ]
