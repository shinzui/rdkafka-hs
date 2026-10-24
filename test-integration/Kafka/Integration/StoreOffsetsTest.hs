module Kafka.Integration.StoreOffsetsTest
  ( tests,
  )
where

import Control.Monad (forM)
import Kafka.Consumer
import Kafka.TestEnv
import Test.Tasty (DependencyType (AllSucceed), TestTree, dependentTestGroup)
import Test.Tasty.HUnit (testCase, (@?=))

tests :: TestEnv -> TestTree
tests env =
  dependentTestGroup
    "Store offsets"
    AllSucceed
    [ produce "1. sends 2 messages to test topic",
      withConsumer env (consumerPropsNoStore env) $ \getConsumer ->
        testCase "2. should receive 2 messages without storing" $ do
          k <- getConsumer
          res <- receiveTestMessages env k 2 4
          length <$> res @?= Right 2

          comRes <- commitAllOffsets OffsetCommit k
          comRes @?= Just (KafkaResponseError RdKafkaRespErrNoOffset),
      produce "3. sends 2 messages to test topic",
      withConsumer env (consumerPropsNoStore env) $ \getConsumer ->
        testCase "4. should receive 4 messages and store them" $ do
          k <- getConsumer
          res <- receiveTestMessages env k 4 4
          storeRes <- forM res . mapM $ storeOffsetMessage k
          comRes <- commitAllOffsets OffsetCommit k

          storeRes @?= Right (replicate 4 Nothing)
          length <$> res @?= Right 4
          comRes @?= Nothing,
      produce "5. sends 2 messages to test topic",
      withConsumer env (consumerPropsNoStore env) $ \getConsumer ->
        testCase "6. should receive 2 messages" $ do
          k <- getConsumer
          res <- receiveTestMessages env k 2 8
          storeRes <- forM res $ mapM (storeOffsetMessage k)
          comRes <- commitAllOffsets OffsetCommit k

          length <$> res @?= Right 2
          storeRes @?= Right [Nothing, Nothing]
          comRes @?= Nothing,
      withKafka env (consumerPropsNoStore env) $ \getKafka ->
        dependentTestGroup
          "Part 3 - Consume after committing stored offsets"
          AllSucceed
          [ testCase "5. sends 2 messages to test topic" $ do
              (_, prod) <- getKafka
              res <- sendMessages (testMessages (teTopic env)) prod
              res @?= Right (),
            testCase "6. should receive 2 messages" $ do
              (k, _) <- getKafka
              res <- receiveTestMessages env k 2 10
              storeRes <- forM res $ mapM (storeOffsetMessage k)
              comRes <- commitAllOffsets OffsetCommit k

              length <$> res @?= Right 2
              storeRes @?= Right [Nothing, Nothing]
              comRes @?= Nothing
          ]
    ]
  where
    produce name =
      withProducer env $ \getProducer ->
        testCase name $ do
          res <- getProducer >>= sendMessages (testMessages (teTopic env))
          res @?= Right ()
