module Main (main) where

import Kafka.Integration.AssignOnlyTest qualified as AssignOnlyTest
import Kafka.Integration.AttributedErrorTest qualified as AttributedErrorTest
import Kafka.Integration.BatchTest qualified as BatchTest
import Kafka.Integration.ConfigurationTest qualified as ConfigurationTest
import Kafka.Integration.ConsumerTest qualified as ConsumerTest
import Kafka.Integration.FatalErrorTest qualified as FatalErrorTest
import Kafka.Integration.HeadersTest qualified as HeadersTest
import Kafka.Integration.LifecycleTest qualified as LifecycleTest
import Kafka.Integration.PerMessageCommitTest qualified as PerMessageCommitTest
import Kafka.Integration.QueueEventTest qualified as QueueEventTest
import Kafka.Integration.StoreOffsetsTest qualified as StoreOffsetsTest
import Kafka.Integration.TopicTest qualified as TopicTest
import Kafka.TestEnv (checkBrokerReachable, newTestEnv, resolveBrokers)
import System.Environment (lookupEnv)
import System.Exit (die)
import Test.Tasty (DependencyType (AllFinish), defaultMain, dependentTestGroup, localOption, mkTimeout, testGroup)
import Test.Tasty.Runners (NumThreads (..))

main :: IO ()
main = do
  brokers <- lookupEnv "RDKAFKA_TEST_BROKER" >>= either die pure . resolveBrokers
  env <- newTestEnv brokers
  checkBrokerReachable env >>= either (die . ("rdkafka-hs-integration: " <>)) pure
  defaultMain
    . localOption (NumThreads 1)
    . localOption (mkTimeout 60000000)
    $ testGroup
      "rdkafka-hs-integration"
      [ ConfigurationTest.tests,
        LifecycleTest.tests env,
        -- The groups share the run's topic and consumer group, so later groups
        -- depend on the records and offsets earlier ones leave behind.
        dependentTestGroup
          "broker"
          AllFinish
          [ PerMessageCommitTest.tests env,
            StoreOffsetsTest.tests env,
            ConsumerTest.tests env,
            BatchTest.tests env,
            HeadersTest.tests env,
            FatalErrorTest.tests env,
            QueueEventTest.tests env,
            AssignOnlyTest.tests env,
            AttributedErrorTest.tests env,
            TopicTest.tests env
          ]
      ]
