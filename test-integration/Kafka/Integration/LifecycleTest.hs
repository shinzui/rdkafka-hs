-- | Client-lifecycle bindings against an address nothing listens on, so no
-- message can ever be delivered.
module Kafka.Integration.LifecycleTest
  ( tests,
  )
where

import Control.Concurrent.MVar (newEmptyMVar, tryPutMVar, tryReadMVar)
import Control.Exception (bracket)
import Control.Monad (void)
import GHC.Clock (getMonotonicTime)
import Kafka.Consumer qualified as C
import Kafka.Producer qualified as P
import Kafka.TestEnv (TestEnv, makeGroupId, prefixedName)
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertBool, assertFailure, testCase, (@?=))

-- | Port 1 on loopback: privileged, so nothing in the test environment
-- listens there.
unreachable :: P.BrokerAddress
unreachable = P.BrokerAddress "127.0.0.1:1"

tests :: TestEnv -> TestTree
tests env =
  testGroup
    "lifecycle"
    [ testCase "flushProducerTimeout times out, purgeProducer empties the queue" $ do
        reports <- newEmptyMVar
        let props =
              P.brokersList [unreachable]
                <> P.setCallback (P.deliveryCallback (void . tryPutMVar reports))
        withProducer props $ \prod -> do
          let record =
                P.ProducerRecord
                  { P.prTopic = P.TopicName (prefixedName env "lifecycle"),
                    P.prPartition = P.UnassignedPartition,
                    P.prKey = Nothing,
                    P.prValue = Just "never delivered",
                    P.prHeaders = mempty
                  }
          P.produceMessage prod record >>= (@?= Nothing)

          start <- getMonotonicTime
          flushed <- P.flushProducerTimeout prod (P.Timeout 500)
          elapsed <- subtract start <$> getMonotonicTime
          flushed @?= Just (P.KafkaResponseError P.RdKafkaRespErrTimedOut)
          assertBool ("flush took " <> show elapsed <> "s") (elapsed >= 0.4 && elapsed < 2)

          P.purgeProducer prod [P.PurgeQueue] >>= (@?= Nothing)
          P.flushProducerTimeout prod (P.Timeout 100) >>= (@?= Nothing)

          tryReadMVar reports >>= \case
            Just (P.DeliveryFailure _ err) -> err @?= P.KafkaResponseError P.RdKafkaRespErrPurgeQueue
            other -> assertFailure ("expected a purged delivery report, got " <> show other),
      testCase "consumerClosed is False until closeConsumer" $ do
        consumer <- newConsumer
        C.consumerClosed consumer >>= (@?= False)
        void (C.closeConsumer consumer)
        C.consumerClosed consumer >>= (@?= True),
      testCase "setSaslCredentials replaces credentials on a consumer" $ do
        consumer <- newConsumer
        C.setSaslCredentials consumer "user-1" "secret-1" >>= (@?= Nothing)
        C.setSaslCredentials consumer "user-2" "secret-2" >>= (@?= Nothing)
        void (C.closeConsumer consumer),
      testCase "setSaslCredentials replaces credentials on a producer" $
        withProducer (P.brokersList [unreachable]) $ \prod -> do
          P.setSaslCredentials prod "user-1" "secret-1" >>= (@?= Nothing)
          P.setSaslCredentials prod "user-2" "secret-2" >>= (@?= Nothing)
    ]
  where
    newConsumer =
      C.newConsumer (C.brokersList [unreachable] <> C.groupId (makeGroupId env "lifecycle")) mempty
        >>= either (assertFailure . ("newConsumer: " <>) . show) pure
    withProducer props =
      bracket
        (P.newProducer props >>= either (assertFailure . ("newProducer: " <>) . show) pure)
        -- Purge before closing: closeProducer flushes, which would wait forever
        -- for a broker that never answers.
        (\prod -> P.purgeProducer prod [P.PurgeQueue, P.PurgeInFlight] >> P.closeProducer prod)
