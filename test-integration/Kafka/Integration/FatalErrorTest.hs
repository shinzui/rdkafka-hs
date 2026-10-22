-- | Regression test for the fork's consumer fatal-error fix.
--
-- A consumer with a @group.instance.id@ is a static group member. When a
-- second consumer joins the same group with the same instance id, the broker
-- fences the first one and librdkafka raises the fatal error
-- 'RdKafkaRespErrFencedInstanceId' on it. librdkafka delivers that only as a
-- consumer error on the consumer queue, never through @error_cb@, so before
-- the fix a consumer in 'CallbackPollModeAsync' could not observe it at all.
-- Both poll modes must now report @Left ('KafkaResponseError'
-- 'RdKafkaRespErrFatal')@ from every poll, with the cause recoverable from
-- 'consumerFatalError'.
module Kafka.Integration.FatalErrorTest
  ( tests,
  )
where

import Control.Concurrent (threadDelay)
import Control.Exception (finally)
import Control.Monad (forM_, replicateM, void)
import Data.Either (isRight)
import Data.Map qualified as Map
import Data.Text qualified as Text
import Kafka.Consumer
import Kafka.Producer (closeProducer)
import Kafka.TestEnv
import Kafka.Topic (NewTopic (..), PartitionCount (..), ReplicationFactor (..), createTopic, deleteTopic)
import System.Timeout (timeout)
import Test.Tasty (TestTree, localOption, mkTimeout, testGroup)
import Test.Tasty.HUnit (Assertion, assertBool, assertFailure, testCase, (@?=))

tests :: TestEnv -> TestTree
tests env =
  localOption (mkTimeout 120000000) $
    testGroup
      "fatal"
      [ testCase "async" (fencedStaticMember env CallbackPollModeAsync),
        testCase "sync" (fencedStaticMember env CallbackPollModeSync)
      ]

fatal :: KafkaError
fatal = KafkaResponseError RdKafkaRespErrFatal

fencedStaticMember :: TestEnv -> CallbackPollMode -> Assertion
fencedStaticMember env mode = do
  let label = case mode of
        CallbackPollModeAsync -> "async"
        CallbackPollModeSync -> "sync"
      topic = TopicName (prefixedName env ("fatal-" <> label))
      props =
        consumerProps env
          <> groupId (makeGroupId env ("fatal-" <> label))
          <> extraProps (Map.fromList [("group.instance.id", prefixedName env ("fatal-" <> label <> "-member"))])
          -- Last: the ConsumerProperties semigroup takes the right operand's
          -- poll mode.
          <> callbackPollMode mode

  admin <- mkProducer env
  created <- createTopic admin (NewTopic topic (PartitionCount 1) (ReplicationFactor 1) mempty)
  assertBool ("createTopic: " <> show created) (isRight created)

  flip finally (void (deleteTopic admin topic) >> closeProducer admin) $ do
    a <- mkConsumerFor topic props
    flip finally (void (closeConsumer a)) $ do
      -- B replaces A's static membership; the broker fences A.
      b <- mkConsumerFor topic props
      flip finally (void (closeConsumer b)) $ do
        result <- timeout 30000000 (awaitFatal a b)
        case result of
          Nothing -> assertFailure "consumer A did not report a fatal error within 30 seconds of being fenced"
          Just () -> pure ()

        again <- pollMessage a (Timeout 100)
        either Just (const Nothing) again @?= Just fatal

        -- The attributed poll reports the same fatal error, with no origin.
        pollMessageAttributed a (Timeout 100) >>= \case
          Left err -> err @?= ConsumerMessageError fatal Nothing Nothing Nothing
          Right record -> assertFailure ("unexpected record " <> show record)

        cause <- consumerFatalError a
        case cause of
          Just (err, description) -> do
            err @?= KafkaResponseError RdKafkaRespErrFencedInstanceId
            assertBool "expected a non-empty fatal error description" (not (Text.null description))
          Nothing -> assertFailure "consumerFatalError returned Nothing after a fatal poll"

        case mode of
          CallbackPollModeAsync -> do
            batches <- replicateM 3 (pollMessageBatch a (Timeout 100) (BatchSize 10))
            forM_ batches $ \batch ->
              (either Just (const Nothing) <$> batch) @?= [Just fatal]
          CallbackPollModeSync -> pure ()

-- | Poll A, and keep B's membership active, until A's poll reports the fatal
-- error. In sync mode A's own poll is also what serves its callbacks.
awaitFatal :: KafkaConsumer -> KafkaConsumer -> IO ()
awaitFatal a b = loop
  where
    loop = do
      res <- pollMessage a (Timeout 200)
      case res of
        Left err | err == fatal -> pure ()
        _ -> do
          void (pollMessage b (Timeout 200))
          threadDelay 50000
          loop
