module Kafka.Integration.PerMessageCommitTest
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
    "Per-message commit"
    AllSucceed
    [ withProducer env $ \getProducer ->
        testCase "1. sends 2 messages to test topic" $ do
          res <- getProducer >>= sendMessages (testMessages (teTopic env))
          res @?= Right (),
      withConsumer env (consumerProps env) $ \getConsumer ->
        testCase "2. should receive 2 messages" $ do
          k <- getConsumer
          res <- receiveMessages k
          length <$> res @?= Right 2

          comRes <- forM res . mapM $ commitOffsetMessage OffsetCommit k
          comRes @?= Right [Nothing, Nothing],
      withProducer env $ \getProducer ->
        testCase "3. sends 2 messages to test topic" $ do
          res <- getProducer >>= sendMessages (testMessages (teTopic env))
          res @?= Right (),
      withConsumer env (consumerProps env) $ \getConsumer ->
        testCase "4. should receive 2 messages again" $ do
          k <- getConsumer
          res <- receiveMessages k
          comRes <- commitAllOffsets OffsetCommit k

          length <$> res @?= Right 2
          comRes @?= Nothing
    ]
