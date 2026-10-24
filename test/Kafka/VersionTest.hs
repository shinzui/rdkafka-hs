module Kafka.VersionTest
  ( tests,
  )
where

import Data.Text qualified as Text
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertBool, testCase, (@?=))
import "rdkafka-hs" Kafka.Version

tests :: TestTree
tests =
  testGroup
    "Kafka.Version"
    [ testCase "linked librdkafka is at least 2.15.0" $
        assertBool
          ("linked librdkafka " <> show librdkafkaVersionNumber <> " is older than 2.15")
          (librdkafkaVersionNumber >= 0x020f0000),
      testCase "version string agrees with the version number" $ do
        let LibrdkafkaVersion {versionMajor, versionMinor} = librdkafkaVersionParts
        take 2 (Text.splitOn "." librdkafkaVersion) @?= [Text.pack (show versionMajor), Text.pack (show versionMinor)],
      testCase "decodes the hex version encoding" $
        decodeLibrdkafkaVersion 0x020f01ff @?= LibrdkafkaVersion {versionMajor = 2, versionMinor = 15, versionRevision = 1}
    ]
