module Main (main) where

import Control.Concurrent (threadDelay)
import Control.Exception (finally)
import Foreign.C.Types (CInt (..))
import Kafka.Compatibility.KafkaFixture qualified as KafkaFixture
import Kafka.Compatibility.SecurityFixture qualified as SecurityFixture
import Kafka.FixtureSupportTest qualified as FixtureSupportTest
import Kafka.Integration.AssignOnlyTest qualified as AssignOnlyTest
import Kafka.Integration.AttributedErrorTest qualified as AttributedErrorTest
import Kafka.Integration.AuthenticationTest qualified as AuthenticationTest
import Kafka.Integration.BatchTest qualified as BatchTest
import Kafka.Integration.BrokerFailureTest qualified as BrokerFailureTest
import Kafka.Integration.CallbackLifetimeTest qualified as CallbackLifetimeTest
import Kafka.Integration.ConfigurationTest qualified as ConfigurationTest
import Kafka.Integration.ConsumerPropertyTest qualified as ConsumerPropertyTest
import Kafka.Integration.ConsumerTest qualified as ConsumerTest
import Kafka.Integration.DestroyTest qualified as DestroyTest
import Kafka.Integration.HeadersTest qualified as HeadersTest
import Kafka.Integration.LifecycleTest qualified as LifecycleTest
import Kafka.Integration.MetadataContractTest qualified as MetadataContractTest
import Kafka.Integration.ObserverTest qualified as ObserverTest
import Kafka.Integration.PerMessageCommitTest qualified as PerMessageCommitTest
import Kafka.Integration.ProducerPropertyTest qualified as ProducerPropertyTest
import Kafka.Integration.ProducerRegressionTest qualified as ProducerRegressionTest
import Kafka.Integration.PublicConsumerContractTest qualified as PublicConsumerContractTest
import Kafka.Integration.ReadCommittedTest qualified as ReadCommittedTest
import Kafka.Integration.SeekPartitionsTest qualified as SeekPartitionsTest
import Kafka.Integration.StoreOffsetsTest qualified as StoreOffsetsTest
import Kafka.Integration.TopicConfigurationTest qualified as TopicConfigurationTest
import Kafka.Integration.TopicTest qualified as TopicTest
import Kafka.Integration.TransactionErrorTest qualified as TransactionErrorTest
import Kafka.TestEnv (checkBrokerReachable, newTestEnv, resolveBrokers)
import System.Environment (getArgs, lookupEnv, setEnv, withArgs)
import System.Exit (die)
import System.Mem (performGC)
import System.Timeout (timeout)
import Test.Tasty (DependencyType (AllFinish), defaultMain, dependentTestGroup, localOption, mkTimeout, testGroup, withResource)
import Test.Tasty.Runners (NumThreads (..))

main :: IO ()
main = do
  nativeLifetime <- lookupEnv "RDKAFKA_TEST_NATIVE_LIFETIME"
  if nativeLifetime == Just "1" then dispatch `finally` quiesceNativeClients else dispatch

dispatch :: IO ()
dispatch =
  getArgs >>= \case
    ["--producer-callback-probe", mode] -> ProducerRegressionTest.runCallbackProbe mode
    ["--lifetime-callback-probe", kind, label] -> CallbackLifetimeTest.runThrowingProbe kind label
    ["--rebalance-error-probe", protocol, operation, poll, failure] -> PublicConsumerContractTest.runRebalanceProbe protocol operation poll failure
    ["--metadata-cancellation-probe", kind] -> MetadataContractTest.runCancellationProbe kind
    ["--callback-lifetime-native-control"] -> CallbackLifetimeTest.runInvalidAccessControl
    "--native-lifetime" : args -> setEnv "RDKAFKA_TEST_NATIVE_LIFETIME" "1" >> withArgs args main
    "--broker-qualification" : args -> withArgs args brokerMain
    "--authentication-qualification" : args -> withArgs args authenticationMain
    _ -> integrationMain

brokerMain :: IO ()
brokerMain =
  defaultMain . localOption (NumThreads 1) . localOption (mkTimeout 120000000) $
    withResource KafkaFixture.createKafkaFixture KafkaFixture.destroyKafkaFixture BrokerFailureTest.tests

authenticationMain :: IO ()
authenticationMain =
  defaultMain . localOption (NumThreads 1) . localOption (mkTimeout 120000000) $
    withResource SecurityFixture.createSecurityFixture SecurityFixture.destroySecurityFixture AuthenticationTest.tests

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
        CallbackLifetimeTest.tests env,
        TransactionErrorTest.tests env,
        LifecycleTest.tests env,
        TopicConfigurationTest.tests env,
        ProducerRegressionTest.tests env,
        ProducerPropertyTest.tests,
        ConsumerPropertyTest.tests env,
        PublicConsumerContractTest.tests env,
        SeekPartitionsTest.tests env,
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
            AssignOnlyTest.tests env,
            AttributedErrorTest.tests env,
            DestroyTest.tests env,
            ReadCommittedTest.tests env,
            ObserverTest.tests env,
            TopicTest.tests env
          ]
      ]

foreign import ccall safe "rd_kafka_wait_destroyed" waitDestroyed :: CInt -> IO CInt

-- Heap diagnostics must observe completed native destruction, including clients
-- whose managed Haskell wrappers became unreachable at the end of a test.
-- This mode never suppresses findings or treats a shutdown timeout as success.
quiesceNativeClients :: IO ()
quiesceNativeClients = do
  let drain = do
        performGC
        result <- waitDestroyed 100
        if result == 0 then pure () else threadDelay 10000 >> drain
  timeout 10000000 drain >>= maybe (die "native clients did not quiesce before heap diagnostics") pure
