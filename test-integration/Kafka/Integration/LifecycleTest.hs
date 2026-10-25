-- | Client-lifecycle bindings against an address nothing listens on, so no
-- message can ever be delivered.
module Kafka.Integration.LifecycleTest
  ( tests,
  )
where

import Control.Concurrent (forkFinally, forkIOWithUnmask, killThread, threadDelay)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, readMVar, tryPutMVar, tryReadMVar)
import Control.Exception (AsyncException (ThreadKilled), SomeException, bracket, finally, fromException, throwIO, try)
import Control.Monad (void)
import GHC.Clock (getMonotonicTime)
import GHC.Conc (BlockReason (BlockedOnException), ThreadStatus (ThreadBlocked), threadStatus)
import Kafka.Consumer qualified as C
import Kafka.Producer qualified as P
import Kafka.TestEnv (TestEnv, makeGroupId, prefixedName, releaseConsumer)
import Kafka.TransactionTestSupport
import System.Timeout (timeout)
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
    $ [ testCase "flushProducerTimeout times out, purgeProducer empties the queue" $ do
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
      <> [ testCase ("setSaslCredentials rejects NUL in " <> label <> " without changing producer or consumer identity") $ do
             let expected = Just (P.KafkaBadSpecification ("SASL " <> field <> " contains NUL"))
             withProducer (P.brokersList [unreachable]) $ \producer ->
               P.setSaslCredentials producer username password >>= (@?= expected)
             bracket newConsumer releaseConsumer $ \consumer ->
               C.setSaslCredentials consumer username password >>= (@?= expected)
         | (label, field, username, password) <- [("username", "username", "fixture\0other", "password"), ("password", "password", "fixture", "password\0other")]
         ]
      <> [ testCase "setSaslCredentials releases each actual native error after decoding its exact code" $ do
             withProducer (P.brokersList [unreachable]) $ \producer -> withTxFault 300 $ do
               P.setSaslCredentials producer "fixture" "password" >>= (@?= Just (P.KafkaResponseError P.RdKafkaRespErrInvalidArg))
               txStat 0 >>= (@?= 1)
               txStat 1 >>= (@?= 1)
               txStat 2 >>= (@?= 1)
               txStat 8 >>= (@?= 0)
             bracket newConsumer releaseConsumer $ \consumer -> withTxFault 300 $ do
               C.setSaslCredentials consumer "fixture" "password" >>= (@?= Just (C.KafkaResponseError C.RdKafkaRespErrInvalidArg))
               txStat 0 >>= (@?= 1)
               txStat 1 >>= (@?= 1)
               txStat 2 >>= (@?= 1)
               txStat 8 >>= (@?= 0)
         ]
      <> [ testCase ("setSaslCredentials cancellation at error " <> label <> " releases the owned error") $
             withProducer (P.brokersList [unreachable]) $ \producer ->
               withTxFault fault $
                 cancelSetter (P.setSaslCredentials producer "fixture" "password")
         | (label, fault) <- [("decoding", 20), ("acquisition", 301)]
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

cancelSetter :: IO (Maybe P.KafkaError) -> IO ()
cancelSetter action = do
  let bounded io = timeout 5000000 io >>= maybe (assertFailure "credential cancellation deadline") pure
  done <- newEmptyMVar
  worker <- forkIOWithUnmask $ \unmask -> try @SomeException (unmask action) >>= putMVar done
  flip finally (releaseTxBoundary >> bounded (readMVar done)) $ do
    awaitTxBoundary
    txStat 0 >>= (@?= 1)
    txStat 1 >>= (@?= 0)
    killed <- newEmptyMVar
    killer <- forkFinally (killThread worker) (putMVar killed)
    let awaitPending = threadStatus killer >>= \status -> if status == ThreadBlocked BlockedOnException then pure () else threadDelay 1000 >> awaitPending
    bounded awaitPending
    releaseTxBoundary
    bounded (readMVar done) >>= \case
      Left exception -> (fromException exception :: Maybe AsyncException) @?= Just ThreadKilled
      Right _ -> assertFailure "pending credential cancellation was not delivered"
    bounded (readMVar killed) >>= either throwIO pure
    txStat 0 >>= (@?= 1)
    txStat 1 >>= (@?= 1)
    txStat 8 >>= (@?= 0)
