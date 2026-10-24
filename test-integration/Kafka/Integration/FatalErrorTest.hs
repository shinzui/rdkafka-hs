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
import Control.Exception (bracket, finally, throwIO)
import Control.Monad (forM_, replicateM, void)
import Data.Map qualified as Map
import Data.Text qualified as Text
import Kafka.Consumer
import Kafka.TestEnv
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
      props =
        consumerProps env
          <> groupId (makeGroupId env ("fatal-" <> label))
          <> extraProps (Map.fromList [("group.instance.id", prefixedName env ("fatal-" <> label <> "-member"))])
          <> callbackPollMode mode
  withTopic env ("fatal-" <> label) 1 $ \topic producer -> do
    produceTo producer topic 0 "active-member"
    let acquireA = newConsumer props (topics [topic] <> offsetReset Earliest) >>= either throwIO pure
        releaseFenced consumer =
          ( closeConsumer consumer >>= \case
              Nothing -> pure ()
              Just err | err == fatal -> pure ()
              Just err -> throwIO err
          )
            `finally` destroyConsumer consumer
    bracket acquireA releaseFenced $ \a -> do
      first <- pollMessage a (Timeout 10000) >>= either throwIO pure
      (crTopic first, crPartition first, crOffset first, crValue first) @?= (topic, PartitionId 0, Offset 0, Just "active-member")
      assignmentLost a >>= (@?= False)
      -- B replaces A's static membership; the broker fences A.
      bracket (mkConsumerFor topic props) releaseConsumer $ \b -> do
        result <- timeout 30000000 (awaitFatal a b)
        case result of
          Nothing -> assertFailure "consumer A did not report a fatal error within 30 seconds of being fenced"
          Just () -> pure ()

        pollConsumerEvents a (Just (Timeout 0))
        assignmentLost b >>= (@?= False)

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
