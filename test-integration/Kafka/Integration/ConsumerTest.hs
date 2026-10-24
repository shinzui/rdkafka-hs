-- The inherited seek checks cover the deprecated seek.
{-# OPTIONS_GHC -Wno-deprecations #-}

-- | The inherited main consumer scenario: produce, then run the same consumer
-- checks once per callback poll mode.
module Kafka.Integration.ConsumerTest
  ( tests,
  )
where

import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import Control.Monad (forM_)
import Data.Either (isRight)
import Data.Map (fromList)
import Kafka.Consumer
import Kafka.Metadata
import Kafka.Producer
import Kafka.TestEnv
import Test.Tasty (DependencyType (AllSucceed), TestTree, dependentTestGroup)
import Test.Tasty.HUnit (assertBool, assertFailure, testCase, (@?=))

tests :: TestEnv -> TestTree
tests env =
  dependentTestGroup
    "Consumer"
    AllSucceed
    [ withProducer env $ \getProducer ->
        dependentTestGroup
          "Run producer"
          AllSucceed
          [ testCase "sends messages to test topic" $ do
              res <- getProducer >>= sendMessages (testMessages (teTopic env))
              res @?= Right (),
            testCase "sends messages with callback to test topic" $ do
              prod <- getProducer
              var <- newEmptyMVar
              let msg =
                    ProducerRecord
                      { prTopic = TopicName (prefixedName env "callback-topic"),
                        prPartition = UnassignedPartition,
                        prKey = Nothing,
                        prValue = Just "test from producer",
                        prHeaders = mempty
                      }

              res <- produceMessage' prod msg (putMVar var)
              res @?= Right ()
              callbackRes <- flushProducer prod *> takeMVar var
              case callbackRes of
                DeliverySuccess _ _ -> pure ()
                DeliveryFailure _ err -> assertFailure ("delivery failed: " <> show err)
                NoMessageError err -> assertFailure ("no message: " <> show err)
          ],
      withConsumer env (consumerProps env <> groupId (makeGroupId env "async")) $
        dependentTestGroup "Run consumer with async polling" AllSucceed . consumerChecks env,
      withConsumer env (consumerProps env <> groupId (makeGroupId env "sync") <> callbackPollMode CallbackPollModeSync) $
        dependentTestGroup "Run consumer with sync polling" AllSucceed . consumerChecks env
    ]

consumerChecks :: TestEnv -> IO KafkaConsumer -> [TestTree]
consumerChecks env getConsumer =
  [ check "should receive messages" $ \k -> do
      res <- receiveTestMessages env k 14 0
      length <$> res @?= Right 14

      let timestamps = crTimestamp <$> either (const []) id res
      forM_ timestamps $ \ts ->
        assertBool "expected a timestamp" (ts /= NoTimestamp)

      comRes <- commitAllOffsets OffsetCommit k
      comRes @?= Nothing,
    check "should get committed" $ \k -> do
      res <- committed k (Timeout 1000) [(testTopic, PartitionId 0)]
      assertBool (show res) (isRight res),
    check "should get position" $ \k -> do
      res <- position k [(testTopic, PartitionId 0)]
      assertBool (show res) (isRight res),
    check "should get watermark offsets" $ \k -> do
      res <- sequence <$> watermarkOffsets k (Timeout 1000) testTopic
      assertBool (show res) (isRight res)
      length <$> res @?= Right 1,
    check "should return subscription" $ \k -> do
      res <- subscription k
      assertBool (show res) (isRight res)
      length <$> res @?= Right 1,
    check "should return assignment" $ \k -> do
      res <- assignment k
      res @?= Right (fromList [(testTopic, [PartitionId 0])]),
    check "assignment is not lost" $ \k ->
      assignmentLost k >>= (@?= False),
    check "should return all topics metadata" $ \k -> do
      res <- allTopicsMetadata k (Timeout 1000)
      assertBool (show res) (isRight res)
      let filterUserTopics m = m {kmTopics = filter (\t -> topicType (tmTopicName t) == User) (kmTopics m)}
      let res' = fmap filterUserTopics res
      length . kmBrokers <$> res' @?= Right 1

      let topicsLen = either (const 0) (length . kmTopics) res'
      let hasTopic = either (const False) (any (\t -> tmTopicName t == testTopic) . kmTopics) res'

      assertBool "expected user topics" (topicsLen > 0)
      hasTopic @?= True,
    check "should return topic metadata" $ \k -> do
      res <- topicMetadata k (Timeout 2000) testTopic
      assertBool (show res) (isRight res)
      length . kmBrokers <$> res @?= Right 1
      length . kmTopics <$> res @?= Right 1,
    check "should describe all consumer groups" $ \k -> do
      res <- allConsumerGroupsInfo k (Timeout 2000)
      assertBool (show res) (isRight res)
      let groups = either (const []) (fmap giGroup) res
      let prefixedGroups = filter (isTestGroupId env) groups
      assertBool "expected this run's consumer groups" (not (null prefixedGroups)),
    check "should describe a given consumer group" $ \k -> do
      res <- consumerGroupInfo k (Timeout 2000) (testGroupId env)
      fmap giGroup <$> res @?= Right [testGroupId env],
    check "should describe non-existent consumer group" $ \k -> do
      res <- consumerGroupInfo k (Timeout 2000) (makeGroupId env "does-not-exist")
      res @?= Right [],
    check "should read topic offsets for time" $ \k -> do
      res <- topicOffsetsForTime k (Timeout 2000) (Millis 1904057189508) testTopic
      assertBool (show res) (isRight res)
      fmap tpOffset <$> res @?= Right [PartitionOffsetEnd],
    check "should seek and return no error" $ \k -> do
      res <- seek k (Timeout 1000) [TopicPartition testTopic (PartitionId 0) (PartitionOffset 1)]
      res @?= Nothing
      msg <- pollMessage k (Timeout 1000)
      crOffset <$> msg @?= Right (Offset 1),
    check "should seek to the beginning" $ \k -> do
      res <- seek k (Timeout 1000) [TopicPartition testTopic (PartitionId 0) PartitionOffsetBeginning]
      res @?= Nothing
      msg <- pollMessage k (Timeout 1000)
      crOffset <$> msg @?= Right (Offset 0),
    check "should seek to the end" $ \k -> do
      res <- seek k (Timeout 1000) [TopicPartition testTopic (PartitionId 0) PartitionOffsetEnd]
      res @?= Nothing
      msg <- pollMessage k (Timeout 1000)
      let offset = crOffset <$> msg
      assertBool
        ("expected partition EOF or a timeout, got " <> show offset)
        ( offset == Left (KafkaResponseError RdKafkaRespErrPartitionEof)
            || offset == Left (KafkaResponseError RdKafkaRespErrTimedOut)
        ),
    check "should respect out-of-bound offsets (invalid offset)" $ \k -> do
      res <- seek k (Timeout 2000) [TopicPartition testTopic (PartitionId 0) PartitionOffsetInvalid]
      res @?= Nothing
      msg <- pollMessage k (Timeout 1000)
      crOffset <$> msg @?= Right (Offset 0),
    check "should respect out-of-bound offsets (huge offset)" $ \k -> do
      res <- seek k (Timeout 3000) [TopicPartition testTopic (PartitionId 0) (PartitionOffset 123456)]
      res @?= Nothing
      msg <- pollMessage k (Timeout 2000)
      crOffset <$> msg @?= Right (Offset 0)
  ]
  where
    testTopic = teTopic env
    check name f = testCase name (getConsumer >>= f)
