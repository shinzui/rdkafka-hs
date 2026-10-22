module Kafka.Integration.BatchTest
  ( tests,
  )
where

import Control.Monad (forM_)
import Data.Either (isRight)
import Kafka.Consumer
import Kafka.TestEnv
import Test.Tasty (DependencyType (AllSucceed), TestTree, dependentTestGroup)
import Test.Tasty.HUnit (assertBool, testCase, (@?=))

tests :: TestEnv -> TestTree
tests env =
  withConsumer env (consumerProps env <> groupId (makeGroupId env "batch-consumer")) $ \getConsumer ->
    dependentTestGroup
      "Batch consumer"
      AllSucceed
      [ testCase "should consume first batch" $ do
          k <- getConsumer
          res <- pollMessageBatch k (Timeout 1000) (BatchSize 5)
          length res @?= 5
          forM_ res $ \r -> assertBool (show r) (isRight r),
        testCase "should consume second batch with not enough messages" $ do
          k <- getConsumer
          res <- pollMessageBatch k (Timeout 1000) (BatchSize 50)
          let res' = Prelude.filter (/= Left (KafkaResponseError RdKafkaRespErrPartitionEof)) res
          assertBool ("expected fewer than 50, got " <> show (length res')) (length res' < 50)
          forM_ res' $ \r -> assertBool (show r) (isRight r),
        testCase "should consume empty batch when there are no messages" $ do
          k <- getConsumer
          res <- pollMessageBatch k (Timeout 1000) (BatchSize 50)
          length res @?= 0
      ]
