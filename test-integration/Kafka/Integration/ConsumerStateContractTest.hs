{-# OPTIONS_GHC -Wno-deprecations #-}

module Kafka.Integration.ConsumerStateContractTest (tests) where

import Control.Exception (bracket, throwIO)
import Control.Monad (forM_)
import Data.ByteString.Char8 qualified as BS
import Data.Map.Strict qualified as M
import Data.Text qualified as Text
import Kafka.Consumer
import Kafka.TestEnv
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (testCase, (@?=))

tests :: TestEnv -> TestTree
tests env =
  testGroup "consumer-state" $
    [ testGroup label $
        [ scenario "assignment-replacement" $ \topic consumer -> do
            subscription consumer >>= (@?= Right [])
            assignment consumer >>= (@?= Right M.empty)
            assign consumer [part topic 0 (PartitionOffset 0)] >>= (@?= Nothing)
            record consumer topic 0 0
            assign consumer [part topic 1 (PartitionOffset 1)] >>= (@?= Nothing)
            assignment consumer >>= (@?= Right (M.singleton topic [PartitionId 1]))
            record consumer topic 1 1
            assign consumer [] >>= (@?= Nothing)
            assignment consumer >>= (@?= Right M.empty)
            pollMessage consumer (Timeout 100) >>= (@?= Left (KafkaResponseError RdKafkaRespErrTimedOut)),
          scenario "empty-requests-preserve-empty-assignment" $ \_ consumer -> do
            pausePartitions consumer [] >>= (@?= KafkaResponseError RdKafkaRespErrNoError)
            resumePartitions consumer [] >>= (@?= KafkaResponseError RdKafkaRespErrNoError)
            storeOffsets consumer [] >>= (@?= Nothing)
            position consumer [] >>= (@?= Right [])
            assignment consumer >>= (@?= Right M.empty),
          scenario "unknown-position-is-invalid-without-native-error" $ \topic consumer -> do
            position consumer [(topic, PartitionId 42)] >>= (@?= Right [part topic 42 PartitionOffsetInvalid]),
          scenario "pause-resume-preserves-records" $ \topic consumer -> do
            assign consumer [part topic 0 (PartitionOffset 0)] >>= (@?= Nothing)
            record consumer topic 0 0
            pausePartitions consumer [(topic, PartitionId 0)] >>= (@?= KafkaResponseError RdKafkaRespErrNoError)
            pollMessage consumer (Timeout 100) >>= (@?= Left (KafkaResponseError RdKafkaRespErrTimedOut))
            position consumer [(topic, PartitionId 0)] >>= (@?= Right [part topic 0 (PartitionOffset 1)])
            resumePartitions consumer [(topic, PartitionId 0)] >>= (@?= KafkaResponseError RdKafkaRespErrNoError)
            record consumer topic 0 1
            record consumer topic 0 2,
          scenario "pause-mixed-member-error" $ \topic consumer -> do
            assign consumer [part topic 0 (PartitionOffset 0)] >>= (@?= Nothing)
            record consumer topic 0 0
            pausePartitions consumer [(topic, PartitionId 0), (unknown topic, PartitionId 42)] >>= (@?= KafkaResponseError RdKafkaRespErrUnknownPartition)
            pollMessage consumer (Timeout 100) >>= (@?= Left (KafkaResponseError RdKafkaRespErrTimedOut))
            resumePartitions consumer [(topic, PartitionId 0)] >>= (@?= KafkaResponseError RdKafkaRespErrNoError)
            record consumer topic 0 1,
          scenario "resume-mixed-member-error" $ \topic consumer -> do
            assign consumer [part topic 0 (PartitionOffset 0)] >>= (@?= Nothing)
            record consumer topic 0 0
            pausePartitions consumer [(topic, PartitionId 0)] >>= (@?= KafkaResponseError RdKafkaRespErrNoError)
            resumePartitions consumer [(unknown topic, PartitionId 42), (topic, PartitionId 0)] >>= (@?= KafkaResponseError RdKafkaRespErrUnknownPartition)
            record consumer topic 0 1,
          scenario "store-commit-next-offset" $ \topic consumer -> do
            assign consumer [part topic 0 (PartitionOffset 0)] >>= (@?= Nothing)
            position consumer [(topic, PartitionId 0)] >>= (@?= Right [part topic 0 PartitionOffsetInvalid])
            committed consumer (Timeout 3000) [(topic, PartitionId 0)] >>= (@?= Right [part topic 0 PartitionOffsetInvalid])
            message <- pollMessage consumer (Timeout 10000) >>= either throwIO pure
            (crOffset message, crValue message) @?= (Offset 0, Just (payload 0 0))
            commitAllOffsets OffsetCommit consumer >>= (@?= Just (KafkaResponseError RdKafkaRespErrNoOffset))
            storeOffsetMessage consumer message >>= (@?= Nothing)
            committed consumer (Timeout 3000) [(topic, PartitionId 0)] >>= (@?= Right [part topic 0 PartitionOffsetInvalid])
            commitAllOffsets OffsetCommit consumer >>= (@?= Nothing)
            committed consumer (Timeout 3000) [(topic, PartitionId 0)] >>= (@?= Right [part topic 0 (PartitionOffset 1)])
            commitPartitionsOffsets OffsetCommit consumer [part topic 0 (PartitionOffset 2)] >>= (@?= Nothing)
            committed consumer (Timeout 3000) [(topic, PartitionId 0)] >>= (@?= Right [part topic 0 (PartitionOffset 2)]),
          scenario "store-mixed-member-error" $ \topic consumer -> do
            assign consumer [part topic 0 (PartitionOffset 0)] >>= (@?= Nothing)
            record consumer topic 0 0
            storeOffsets consumer [part topic 0 (PartitionOffset 2), part (unknown topic) 42 (PartitionOffset 1)] >>= (@?= Just (KafkaResponseError RdKafkaRespErrUnknownPartition))
            commitAllOffsets OffsetCommit consumer >>= (@?= Nothing)
            committed consumer (Timeout 3000) [(topic, PartitionId 0)] >>= (@?= Right [part topic 0 (PartitionOffset 2)]),
          scenario "position-mixed-member-error" $ \topic consumer -> do
            assign consumer [part topic 0 (PartitionOffset 0)] >>= (@?= Nothing)
            record consumer topic 0 0
            position consumer [(topic, PartitionId 0), (TopicName (Text.replicate 513 "x"), PartitionId 0)] >>= (@?= Left (KafkaResponseError RdKafkaRespErrUnknownPartition)),
          scenario "deprecated-seek-exact-repeated-replay" $ \topic consumer -> do
            assign consumer [part topic 0 (PartitionOffset 0)] >>= (@?= Nothing)
            forM_ [0 .. 2] (record consumer topic 0)
            forM_ ([0 .. 19] :: [Int]) $ \iteration -> do
              let offset = iteration `mod` 3
              seek consumer (Timeout 2000) [part topic 0 (PartitionOffset (fromIntegral offset))] >>= (@?= Nothing)
              record consumer topic 0 offset,
          testCase "explicit-subscription-overrides-global-reset" $
            withTopic env (Text.pack ("state-" <> label <> "-explicit-reset")) 1 $ \topic producer -> do
              produceTo producer topic 0 "existing"
              bracket (newConsumer (base "explicit-reset" <> extraProp "auto.offset.reset" "latest") (topics [topic] <> offsetReset Earliest) >>= either throwIO pure) releaseConsumer $ \consumer -> do
                message <- pollMessage consumer (Timeout 10000) >>= either throwIO pure
                (crTopic message, crOffset message, crValue message) @?= (topic, Offset 0, Just "existing"),
          testCase "regex-subscription-selects-run-owned-topic" $
            withTopic env (Text.pack ("state-" <> label <> "-regex")) 1 $ \topic@(TopicName name) producer -> do
              produceTo producer topic 0 "matched"
              let patternTopic = TopicName ("^" <> name <> "$")
              bracket (newConsumer (base "regex") (topics [patternTopic] <> offsetReset Earliest) >>= either throwIO pure) releaseConsumer $ \consumer -> do
                message <- pollMessage consumer (Timeout 10000) >>= either throwIO pure
                (crTopic message, crOffset message, crValue message) @?= (topic, Offset 0, Just "matched")
                subscription consumer >>= (@?= Right [(patternTopic, SubscribedPartitionsAll)]),
          testCase "global-topic-setting-survives-empty-subscription-properties" $
            withTopic env (Text.pack ("state-" <> label <> "-global-reset")) 1 $ \topic producer -> do
              produceTo producer topic 0 "existing"
              let props = base "global-reset" <> extraProp "auto.offset.reset" "earliest"
              bracket (newConsumer props (topics [topic]) >>= either throwIO pure) releaseConsumer $ \consumer -> do
                message <- pollMessage consumer (Timeout 3000) >>= either throwIO pure
                (crTopic message, crOffset message, crValue message) @?= (topic, Offset 0, Just "existing")
        ]
    | (label, mode) <- [("async", CallbackPollModeAsync), ("sync", CallbackPollModeSync)],
      let base name = brokersList (teBrokers env) <> groupId (makeGroupId env (Text.pack ("state-" <> label <> "-" <> name))) <> noAutoCommit <> noAutoOffsetStore <> callbackPollMode mode,
      let scenario name action = testCase name $
            withTopic env (Text.pack ("state-" <> label <> "-" <> name)) 2 $ \topic producer -> do
              forM_ [0, 1] $ \partition -> forM_ [0 .. 2] $ \offset -> produceTo producer topic partition (payload partition offset)
              bracket (newAssignedConsumer (base name) >>= either throwIO pure) releaseConsumer (action topic)
    ]

part :: TopicName -> Int -> PartitionOffset -> TopicPartition
part topic partition = TopicPartition topic (PartitionId partition)

payload :: Int -> Int -> BS.ByteString
payload partition offset = BS.pack (show partition <> ":" <> show offset)

record :: KafkaConsumer -> TopicName -> Int -> Int -> IO ()
record consumer topic partition offset = do
  message <- pollMessage consumer (Timeout 10000) >>= either throwIO pure
  (crTopic message, crPartition message, crOffset message, crKey message, crValue message, crHeaders message)
    @?= (topic, PartitionId partition, Offset (fromIntegral offset), Nothing, Just (payload partition offset), mempty)

unknown :: TopicName -> TopicName
unknown (TopicName topic) = TopicName (topic <> "-absent")
