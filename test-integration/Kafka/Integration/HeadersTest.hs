module Kafka.Integration.HeadersTest
  ( tests,
  )
where

import Control.Monad (forM_, void)
import Data.Set qualified as Set
import Kafka.Consumer
import Kafka.TestEnv
import Test.Tasty (DependencyType (AllSucceed), TestTree, dependentTestGroup)
import Test.Tasty.HUnit (testCase, (@?=))

tests :: TestEnv -> TestTree
tests env =
  withKafka env (consumerProps env) $ \getKafka ->
    dependentTestGroup
      "Headers consumer/producer"
      AllSucceed
      [ testCase "1. sends 2 messages to test topic enriched with headers" $ do
          (k, prod) <- getKafka
          void $ receiveMessages k

          res <- sendMessagesWithHeaders (testMessages (teTopic env)) testHeaders prod
          res @?= Right (),
        testCase "2. should receive 2 messages enriched with headers" $ do
          (k, _) <- getKafka
          res <- receiveMessages k
          (length <$> res) @?= Right 2

          forM_ res $ \rcs ->
            forM_ rcs $ \rc ->
              Set.fromList (headersToList (crHeaders rc)) @?= Set.fromList (headersToList testHeaders)
      ]
  where
    testHeaders = headersFromList [("a-header-name", "a-header-value"), ("b-header-name", "b-header-value")]
