-- | An assign-only consumer reads explicitly assigned partitions without
-- joining a group (improvement request IR-2, AC-1), in both callback poll
-- modes.
module Kafka.Integration.AssignOnlyTest
  ( tests,
  )
where

import Control.Exception (finally)
import Control.Monad (void)
import Data.ByteString (ByteString)
import Data.Either (isRight)
import Data.Text qualified as Text
import GHC.Clock (getMonotonicTime)
import Kafka.Consumer
import Kafka.Metadata (allConsumerGroupsInfo, giGroup)
import Kafka.TestEnv
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (Assertion, assertBool, assertFailure, testCase, (@?=))

tests :: TestEnv -> TestTree
tests env =
  testGroup
    "assign-only"
    [ testGroup
        label
        [ testCase "newAssignedConsumer" $ scenario env mode (label <> "-alias") newAssignedConsumer,
          testCase "newConsumer with an empty subscription" $
            scenario env mode (label <> "-empty") (`newConsumer` mempty)
        ]
    | (label, mode) <- [("async", CallbackPollModeAsync), ("sync", CallbackPollModeSync)]
    ]

scenario ::
  TestEnv ->
  CallbackPollMode ->
  String ->
  (ConsumerProperties -> IO (Either KafkaError KafkaConsumer)) ->
  Assertion
scenario env mode label construct =
  withTopic env (Text.pack ("assign-only-" <> label)) 1 $ \topic producer -> do
    produceTo producer topic 0 "r0"
    produceTo producer topic 0 "r1"
    let group = makeGroupId env (Text.pack ("assign-only-" <> label))
        props =
          consumerProps env
            <> groupId group
            <> noAutoCommit
            <> callbackPollMode mode
    consumer <- construct props >>= either (assertFailure . ("constructor: " <>) . show) pure
    flip finally (void (closeConsumer consumer)) $ do
      assign consumer [TopicPartition topic (PartitionId 0) (PartitionOffset 0)] >>= (@?= Nothing)
      nextValue consumer "r0" >>= (@?= "r0")

      -- Reassigning at another explicit offset keeps routing correct.
      assign consumer [TopicPartition topic (PartitionId 0) (PartitionOffset 1)] >>= (@?= Nothing)
      nextValue consumer "r1" >>= (@?= "r1")

      groups <- allConsumerGroupsInfo consumer (Timeout 5000)
      assertBool ("allConsumerGroupsInfo: " <> show groups) (isRight groups)
      assertBool
        ("the assign-only group " <> show group <> " was registered with the broker")
        (group `notElem` either (const []) (fmap giGroup) groups)

-- | The value of the next record, polling for at most ten seconds.
nextValue :: KafkaConsumer -> String -> IO ByteString
nextValue consumer expected = do
  deadline <- (+ 10) <$> getMonotonicTime
  let go = do
        now <- getMonotonicTime
        if now > deadline
          then assertFailure ("no record arrived within 10 seconds (expected " <> expected <> ")")
          else
            pollMessage consumer (Timeout 500) >>= \case
              Right record -> maybe go pure (crValue record)
              Left _ -> go
  go
