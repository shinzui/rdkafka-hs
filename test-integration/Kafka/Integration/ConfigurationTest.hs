-- | Broker selection is checked before any client exists, so these tests need
-- no network access.
module Kafka.Integration.ConfigurationTest
  ( tests,
  )
where

import Data.Either (isLeft)
import Kafka.Consumer (BrokerAddress (..))
import Kafka.TestEnv (resolveBrokers)
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertBool, testCase, (@?=))

tests :: TestTree
tests =
  testGroup
    "configuration"
    [ testCase "defaults to the private broker" $
        resolveBrokers Nothing @?= Right [BrokerAddress "127.0.0.1:39092"],
      testCase "accepts another private endpoint" $
        resolveBrokers (Just "127.0.0.1:1") @?= Right [BrokerAddress "127.0.0.1:1"],
      testCase "accepts a list of private endpoints" $
        resolveBrokers (Just "127.0.0.1:39092, 127.0.0.1:39093")
          @?= Right [BrokerAddress "127.0.0.1:39092", BrokerAddress "127.0.0.1:39093"],
      testCase "rejects an empty value" $
        assertRejected "",
      testCase "rejects localhost:9092" $
        assertRejected "localhost:9092",
      testCase "rejects 127.0.0.1:9092" $
        assertRejected "127.0.0.1:9092",
      testCase "rejects [::1]:9092" $
        assertRejected "[::1]:9092",
      testCase "rejects a loopback host without a port" $
        assertRejected "localhost",
      testCase "rejects the shared broker anywhere in a list" $
        assertRejected "127.0.0.1:39092,LOCALHOST:9092"
    ]
  where
    assertRejected value =
      assertBool ("expected " <> show value <> " to be rejected") (isLeft (resolveBrokers (Just value)))
