module Kafka.Consumer.ConsumerPropertiesSpec
  ( tests,
  )
where

import Kafka.Consumer.ConsumerProperties
import Kafka.Consumer.Types (ConsumerGroupId (..))
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (testCase, (@?=))

tests :: TestTree
tests =
  testGroup
    "Kafka.Consumer.ConsumerProperties"
    [ testCase "the poll mode defaults to async" $
        effectiveCallbackPollMode (groupId (ConsumerGroupId "g")) @?= CallbackPollModeAsync,
      testCase "properties composed after the poll mode keep it" $
        effectiveCallbackPollMode
          (callbackPollMode CallbackPollModeSync <> groupId (ConsumerGroupId "g") <> noAutoCommit)
          @?= CallbackPollModeSync,
      testCase "properties composed before the poll mode keep it" $
        effectiveCallbackPollMode (groupId (ConsumerGroupId "g") <> callbackPollMode CallbackPollModeSync)
          @?= CallbackPollModeSync,
      testCase "a later poll mode overrides an earlier one" $
        effectiveCallbackPollMode (callbackPollMode CallbackPollModeSync <> callbackPollMode CallbackPollModeAsync)
          @?= CallbackPollModeAsync
    ]
