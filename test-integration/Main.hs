module Main (main) where

import Kafka.FixtureSupportTest qualified as FixtureSupportTest
import Kafka.Integration.AssignOnlyTest qualified as AssignOnlyTest
import Kafka.Integration.AttributedErrorTest qualified as AttributedErrorTest
import Kafka.Integration.BatchTest qualified as BatchTest
import Kafka.Integration.ConfigurationTest qualified as ConfigurationTest
import Kafka.Integration.ConsumerTest qualified as ConsumerTest
import Kafka.Integration.DestroyTest qualified as DestroyTest
import Kafka.Integration.FatalErrorTest qualified as FatalErrorTest
import Kafka.Integration.HeadersTest qualified as HeadersTest
import Kafka.Integration.LifecycleTest qualified as LifecycleTest
import Kafka.Integration.ObserverTest qualified as ObserverTest
import Kafka.Integration.PerMessageCommitTest qualified as PerMessageCommitTest
import Kafka.Integration.ProducerPropertyTest qualified as ProducerPropertyTest
import Kafka.Integration.ProducerRegressionTest qualified as ProducerRegressionTest
import Kafka.Integration.QueueEventTest qualified as QueueEventTest
import Kafka.Integration.ReadCommittedTest qualified as ReadCommittedTest
import Kafka.Integration.StoreOffsetsTest qualified as StoreOffsetsTest
import Kafka.Integration.TopicConfigurationTest qualified as TopicConfigurationTest
import Kafka.Integration.TopicTest qualified as TopicTest
import Kafka.TestEnv (checkBrokerReachable, newTestEnv, resolveBrokers)
import System.Environment (getArgs, lookupEnv)
import System.Exit (die)
import Test.Tasty (DependencyType (AllFinish), defaultMain, dependentTestGroup, localOption, mkTimeout, testGroup)
import Test.Tasty.Runners (NumThreads (..))

main :: IO ()
main =
  getArgs >>= \case
    ["--producer-callback-probe", mode] -> ProducerRegressionTest.runCallbackProbe mode
    _ -> integrationMain

integrationMain :: IO ()
integrationMain = do
  brokers <- lookupEnv "RDKAFKA_TEST_BROKER" >>= either die pure . resolveBrokers
  env <- newTestEnv brokers
  checkBrokerReachable env >>= either (die . ("rdkafka-hs-integration: " <>)) pure
  defaultMain
    . localOption (NumThreads 1)
    . localOption (mkTimeout 60000000)
    $ testGroup
      "rdkafka-hs-integration"
      [ FixtureSupportTest.tests,
        ConfigurationTest.tests,
        LifecycleTest.tests env,
        TopicConfigurationTest.tests env,
        ProducerRegressionTest.tests env,
        ProducerPropertyTest.tests,
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
            DestroyTest.tests env,
            ReadCommittedTest.tests env,
            ObserverTest.tests env,
            TopicTest.tests env
          ]
      ]
