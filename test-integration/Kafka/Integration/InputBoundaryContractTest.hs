{-# OPTIONS_GHC -Wno-deprecations #-}

module Kafka.Integration.InputBoundaryContractTest (tests) where

import Control.Exception (bracket, throwIO)
import Control.Monad (void)
import Data.Map.Strict qualified as Map
import Data.Text qualified as Text
import Kafka.Consumer
import Kafka.Metadata
import Kafka.TestEnv
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit

tests :: TestEnv -> TestTree
tests env =
  testGroup "input-boundaries" $
    [ testCase (label <> " " <> name <> " rejects " <> invalid) $
        withTopic env (Text.pack ("input-" <> label <> "-" <> name <> "-" <> invalid)) 2 $ \topic producer -> do
          produceTo producer topic 1 "first"
          produceTo producer topic 1 "second"
          let props = brokersList (teBrokers env) <> groupId (makeGroupId env (Text.pack ("input-" <> label <> "-" <> name <> "-" <> invalid))) <> noAutoCommit <> noAutoOffsetStore <> callbackPollMode mode
          bracket (newAssignedConsumer props >>= either throwIO pure) releaseConsumer $ \consumer -> do
            assign consumer [TopicPartition topic (PartitionId 1) (PartitionOffset 0)] >>= (@?= Nothing)
            first <- pollMessage consumer (Timeout 10000) >>= either throwIO pure
            (crPartition first, crOffset first, crValue first) @?= (PartitionId 1, Offset 0, Just "first")
            let (badTopic, badPartition, expected) = bad topic
                record = first {crTopic = badTopic, crPartition = badPartition}
            action consumer record >>= (@?= Left expected)
            assignment consumer >>= (@?= Right (Map.singleton topic [PartitionId 1]))
            second <- pollMessage consumer (Timeout 10000) >>= either throwIO pure
            (crTopic second, crPartition second, crOffset second, crValue second) @?= (topic, PartitionId 1, Offset 1, Just "second")
    | (label, mode) <- [("async", CallbackPollModeAsync), ("sync", CallbackPollModeSync)],
      (invalid, bad) <- invalidInputs,
      (name, action) <- operations
    ]
      <> stringTests env
  where
    invalidInputs =
      [ ("nul-topic", \(TopicName name) -> (TopicName (name <> "\0suffix"), PartitionId 0, KafkaBadSpecification "topic name contains NUL")),
        ("overflow-partition", \topic -> (topic, PartitionId 4294967296, KafkaBadSpecification "partition is outside the C int range")),
        ("underflow-partition", \topic -> (topic, PartitionId (-2147483649), KafkaBadSpecification "partition is outside the C int range"))
      ]
    pairs record = [(crTopic record, crPartition record)]
    parts record = [TopicPartition (crTopic record) (crPartition record) (PartitionOffset 0)]
    maybeError action = maybe (Right ()) Left <$> action
    response action = action >>= \err -> pure (if err == KafkaResponseError RdKafkaRespErrNoError then Right () else Left err)
    operations =
      [ ("assign", \c r -> maybeError (assign c (parts r))),
        ("pause", \c r -> response (pausePartitions c (pairs r))),
        ("resume", \c r -> response (resumePartitions c (pairs r))),
        ("store", \c r -> maybeError (storeOffsets c (parts r))),
        ("store-message", \c r -> maybeError (storeOffsetMessage c r)),
        ("commit", \c r -> maybeError (commitPartitionsOffsets OffsetCommit c (parts r))),
        ("commit-message", \c r -> maybeError (commitOffsetMessage OffsetCommit c r)),
        ("committed", \c r -> void <$> committed c (Timeout 200) (pairs r)),
        ("position", \c r -> void <$> position c (pairs r)),
        ("legacy-seek", \c r -> maybeError (seek c (Timeout 200) (parts r))),
        ("seek", \c r -> maybeError (seekPartitions c (parts r) (Timeout 200))),
        ("timestamp", \c r -> void <$> offsetsForTime c (Timeout 200) (Millis 0) (pairs r)),
        ("watermark", \c r -> void <$> partitionWatermarkOffsets c (Timeout 200) (crTopic r) (crPartition r))
      ]

stringTests :: TestEnv -> [TestTree]
stringTests env =
  [ testCase (label <> " string queries and subscription reject NUL before native effects") $
      withTopic env (Text.pack ("input-strings-" <> label)) 1 $ \topic producer -> do
        let bad = TopicName (unTopicName topic <> "\0suffix")
            failure = KafkaBadSpecification "topic name contains NUL"
            partition = PartitionMetadata (PartitionId 0) Nothing (BrokerId 0) [BrokerId 0] [BrokerId 0]
            metadata = TopicMetadata bad [partition] Nothing
            props = brokersList (teBrokers env) <> groupId (makeGroupId env (Text.pack ("input-strings-" <> label))) <> noAutoCommit <> callbackPollMode mode
        topicMetadata producer (Timeout 200) bad >>= (@?= Left failure)
        watermarkOffsets producer (Timeout 200) bad >>= (@?= [Left failure])
        watermarkOffsets' producer (Timeout 200) metadata >>= (@?= [Left failure])
        topicOffsetsForTime producer (Timeout 200) (Millis 0) bad >>= (@?= Left failure)
        offsetsForTime' producer (Timeout 200) (Millis 0) metadata >>= (@?= Left failure)
        consumerGroupInfo producer (Timeout 200) (ConsumerGroupId "group\0suffix") >>= (@?= Left (KafkaBadSpecification "consumer group contains NUL"))
        created <- newConsumer props (topics [bad] <> offsetReset Earliest)
        case created of
          Left err -> err @?= failure
          Right consumer -> releaseConsumer consumer >> assertFailure "subscription accepted a truncated topic"
        bracket (newAssignedConsumer props >>= either throwIO pure) releaseConsumer $ \consumer -> do
          assign consumer [TopicPartition topic (PartitionId 0) (PartitionOffset 0)] >>= (@?= Nothing)
          assign consumer [TopicPartition topic (PartitionId 0) (PartitionOffset 1), TopicPartition bad (PartitionId 0) (PartitionOffset 0)] >>= (@?= Just failure)
          assignment consumer >>= (@?= Right (Map.singleton topic [PartitionId 0]))
          position consumer [(topic, PartitionId 2147483647)] >>= (@?= Right [TopicPartition topic (PartitionId 2147483647) PartitionOffsetInvalid])
  | (label, mode) <- [("async", CallbackPollModeAsync), ("sync", CallbackPollModeSync)]
  ]
