-- | Topic administration. Only topics carrying this run's prefix are ever
-- deleted.
module Kafka.Integration.TopicTest
  ( tests,
  )
where

import Control.Monad (forM_, replicateM)
import Data.Either (isRight)
import Data.IORef (newIORef, readIORef, writeIORef)
import Data.Text qualified as Text
import Kafka.Consumer
import Kafka.Metadata
import Kafka.TestEnv
import Kafka.Topic
import System.Random (randomRIO)
import Test.Tasty (DependencyType (AllSucceed), TestTree, dependentTestGroup, withResource)
import Test.Tasty.HUnit (assertBool, testCase, (@?=))

tests :: TestEnv -> TestTree
tests env =
  withResource (newIORef []) (const (pure ())) $ \getTopicsRef ->
    withConsumer env (consumerProps env) $ \getConsumer ->
      dependentTestGroup
        "Read all topics"
        AllSucceed
        [ testCase "should create a topic" $ do
            consumer <- getConsumer
            suffix <- replicateM 5 (randomRIO ('a', 'z'))
            let newTopic = mkNewTopic (TopicName (prefixedName env ("admin.topic.created." <> Text.pack suffix)))
            result <- createTopic consumer newTopic
            assertBool (show result) (isRight result),
          testCase "should return all the topics" $ do
            consumer <- getConsumer
            res <- allTopicsMetadata consumer (Timeout 1000)
            assertBool (show res) (isRight res)
            let filterUserTopics m = m {kmTopics = filter (\t -> topicType (tmTopicName t) == User) (kmTopics m)}
            let res' = fmap filterUserTopics res
            length . kmBrokers <$> res' @?= Right 1

            let runTopics = filter (isTestTopic env) (either (const []) (map tmTopicName . kmTopics) res')
            getTopicsRef >>= \ref -> writeIORef ref runTopics

            let hasTopic = teTopic env `elem` runTopics
            assertBool "expected this run's topics" (not (null runTopics))
            hasTopic @?= True,
          testCase "should delete this run's topics" $ do
            consumer <- getConsumer
            runTopics <- getTopicsRef >>= readIORef
            forM_ runTopics $ \topic -> do
              result <- deleteTopic consumer topic
              assertBool (show (topic, result)) (isRight result)
        ]

mkNewTopic :: TopicName -> NewTopic
mkNewTopic name = NewTopic name (PartitionCount 1) (ReplicationFactor 1) mempty
