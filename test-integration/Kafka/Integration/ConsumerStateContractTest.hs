{-# OPTIONS_GHC -Wno-deprecations #-}

module Kafka.Integration.ConsumerStateContractTest (tests) where

import Control.Concurrent.MVar (newEmptyMVar, readMVar, tryPutMVar)
import Control.Exception (bracket, throwIO)
import Control.Monad (forM_, void)
import Data.ByteString.Char8 qualified as BS
import Data.List (sort)
import Data.Map.Strict qualified as M
import Data.Text qualified as Text
import Kafka.Consumer
import Kafka.ConsumerStateSupport (triggerCooperative, withStateFault)
import Kafka.MessageTestSupport (messageStat, withMessageFault)
import Kafka.Producer qualified as P
import Kafka.TestEnv
import System.Timeout (timeout)
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertBool, assertFailure, testCase, (@?=))

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
          testCase "unknown-assignment-reports-through-error-callback-and-recovers" $
            withTopic env (Text.pack ("state-" <> label <> "-unknown-assignment")) 1 $ \topic producer -> do
              produceTo producer topic 0 "recovered"
              events <- newEmptyMVar
              let props = base "unknown-assignment" <> setCallback (errorCallback (\err reason -> void (tryPutMVar events (err, reason))))
              bracket (newAssignedConsumer props >>= either throwIO pure) releaseConsumer $ \consumer -> do
                assign consumer [part topic 42 (PartitionOffset 0)] >>= (@?= Nothing)
                assignment consumer >>= (@?= Right (M.singleton topic [PartitionId 42]))
                pollMessage consumer (Timeout 1000) >>= (@?= Left (KafkaResponseError RdKafkaRespErrTimedOut))
                timeout 5000000 (readMVar events) >>= \case
                  Just (err, reason) -> do
                    err @?= KafkaResponseError RdKafkaRespErrUnknownPartition
                    assertBool reason (unTopicName topic `Text.isInfixOf` Text.pack reason)
                  Nothing -> assertFailure "missing native assignment error callback"
                assignmentLost consumer >>= (@?= False)
                assign consumer [part topic 0 (PartitionOffset 0)] >>= (@?= Nothing)
                message <- pollMessage consumer (Timeout 10000) >>= either throwIO pure
                (crTopic message, crPartition message, crOffset message, crValue message) @?= (topic, PartitionId 0, Offset 0, Just "recovered"),
          scenario "representable-partition-boundaries-keep-native-query-semantics" $ \topic consumer -> do
            let requested = [TopicPartition topic (PartitionId p) PartitionOffsetInvalid | p <- [-2147483648, 2147483647]]
            position consumer [(tpTopicName p, tpPartition p) | p <- requested] >>= (@?= Right requested),
          scenario "deprecated-seek-reports-unassigned-state-and-recovers-after-assignment" $ \topic consumer -> do
            assign consumer [part topic 0 (PartitionOffset 0)] >>= (@?= Nothing)
            record consumer topic 0 0
            observed <- assignment consumer
            observed @?= Right (M.singleton topic [PartitionId 0])
            assign consumer [] >>= (@?= Nothing)
            -- The earlier successful query does not establish seek's later
            -- active-fetcher precondition; preserve the native State response.
            seek consumer (Timeout 2000) [part topic 0 (PartitionOffset 1)] >>= (@?= Just (KafkaResponseError RdKafkaRespErrState))
            assignment consumer >>= (@?= Right M.empty)
            assign consumer [part topic 0 (PartitionOffset 2)] >>= (@?= Nothing)
            record consumer topic 0 2,
          testCase "subscription-preserves-the-exact-multiple-topic-set" $
            withTopic env (Text.pack ("state-" <> label <> "-subscription-a")) 1 $ \first producerA ->
              withTopic env (Text.pack ("state-" <> label <> "-subscription-b")) 1 $ \second producerB -> do
                produceTo producerA first 0 "first-topic"
                produceTo producerB second 0 "second-topic"
                bracket (newConsumer (base "multiple-topics" <> logLevel KafkaLogWarning) (topics [second, first, second] <> offsetReset Earliest) >>= either throwIO pure) releaseConsumer $ \consumer -> do
                  subscription consumer >>= (@?= Right [(first, SubscribedPartitionsAll), (second, SubscribedPartitionsAll)])
                  records <- receiveMessages consumer 2 >>= either throwIO pure
                  sort [(crTopic r, crPartition r, crOffset r, crValue r) | r <- records] @?= [(first, PartitionId 0, Offset 0, Just "first-topic"), (second, PartitionId 0, Offset 0, Just "second-topic")],
          scenario "empty-requests-preserve-empty-assignment" $ \_ consumer -> do
            pausePartitions consumer [] >>= (@?= KafkaResponseError RdKafkaRespErrNoError)
            resumePartitions consumer [] >>= (@?= KafkaResponseError RdKafkaRespErrNoError)
            storeOffsets consumer [] >>= (@?= Nothing)
            position consumer [] >>= (@?= Right [])
            assignment consumer >>= (@?= Right M.empty),
          scenario "unknown-position-is-invalid-without-native-error" $ \topic consumer -> do
            position consumer [(topic, PartitionId 42)] >>= (@?= Right [part topic 42 PartitionOffsetInvalid]),
          scenario "multiple-assigned-partitions-retain-the-exact-topic-map" $ \topic consumer -> do
            assign consumer [part topic 0 (PartitionOffset 0), part topic 1 (PartitionOffset 0)] >>= (@?= Nothing)
            fmap (M.map sort) <$> assignment consumer >>= (@?= Right (M.singleton topic [PartitionId 0, PartitionId 1])),
          scenario "legacy-seek-constructor-failure-does-not-execute-an-earlier-valid-seek" $ \topic consumer -> do
            assign consumer [part topic 0 (PartitionOffset 0)] >>= (@?= Nothing)
            record consumer topic 0 0
            seek consumer (Timeout 200) [part topic 0 (PartitionOffset 2), part (TopicName (Text.replicate 513 "x")) 0 (PartitionOffset 0)]
              >>= (@?= Just (KafkaError "Local: Invalid argument or configuration"))
            assignment consumer >>= (@?= Right (M.singleton topic [PartitionId 0]))
            record consumer topic 0 1,
          scenario "pause-resume-preserves-records" $ \topic consumer -> do
            assign consumer [part topic 0 (PartitionOffset 0)] >>= (@?= Nothing)
            record consumer topic 0 0
            pausePartitions consumer [(topic, PartitionId 0)] >>= (@?= KafkaResponseError RdKafkaRespErrNoError)
            pollMessage consumer (Timeout 100) >>= (@?= Left (KafkaResponseError RdKafkaRespErrTimedOut))
            position consumer [(topic, PartitionId 0)] >>= (@?= Right [part topic 0 (PartitionOffset 1)])
            resumePartitions consumer [(topic, PartitionId 0)] >>= (@?= KafkaResponseError RdKafkaRespErrNoError)
            record consumer topic 0 1
            record consumer topic 0 2,
          scenario "callback-event-poll-preserves-application-records" $ \topic consumer -> do
            assign consumer [part topic 0 (PartitionOffset 0)] >>= (@?= Nothing)
            pollConsumerEvents consumer (Just (Timeout 1000))
            forM_ [0 .. 2] (record consumer topic 0),
          scenario "callback-buffer-seek-replaces-pending-records" $ \topic consumer -> do
            assign consumer [part topic 0 (PartitionOffset 0)] >>= (@?= Nothing)
            pollConsumerEvents consumer (Just (Timeout 1000))
            seekPartitions consumer [part topic 0 (PartitionOffset 2)] (Timeout 2000) >>= (@?= Nothing)
            record consumer topic 0 2
            position consumer [(topic, PartitionId 0)] >>= (@?= Right [part topic 0 (PartitionOffset 3)]),
          scenario "callback-buffer-reassignment-replaces-pending-records" $ \topic consumer -> do
            assign consumer [part topic 0 (PartitionOffset 0)] >>= (@?= Nothing)
            pollConsumerEvents consumer (Just (Timeout 1000))
            assign consumer [part topic 1 (PartitionOffset 1)] >>= (@?= Nothing)
            record consumer topic 1 1,
          scenario "callback-buffer-pause-defers-pending-records" $ \topic consumer -> do
            assign consumer [part topic 0 (PartitionOffset 0)] >>= (@?= Nothing)
            pollConsumerEvents consumer (Just (Timeout 1000))
            pausePartitions consumer [(topic, PartitionId 0)] >>= (@?= KafkaResponseError RdKafkaRespErrNoError)
            pollMessage consumer (Timeout 100) >>= (@?= Left (KafkaResponseError RdKafkaRespErrTimedOut))
            resumePartitions consumer [(topic, PartitionId 0)] >>= (@?= KafkaResponseError RdKafkaRespErrNoError)
            forM_ [0 .. 2] (record consumer topic 0),
          scenario "callback-buffer-legacy-seek-replaces-pending-records" $ \topic consumer -> do
            assign consumer [part topic 0 (PartitionOffset 0)] >>= (@?= Nothing)
            pollConsumerEvents consumer (Just (Timeout 1000))
            seek consumer (Timeout 2000) [part topic 0 (PartitionOffset 2)] >>= (@?= Nothing)
            record consumer topic 0 2,
          scenario "callback-buffer-legacy-partial-seek-keeps-success" $ \topic consumer -> do
            assign consumer [part topic 0 (PartitionOffset 0)] >>= (@?= Nothing)
            pollConsumerEvents consumer (Just (Timeout 1000))
            seek consumer (Timeout 2000) [part topic 0 (PartitionOffset 2), part topic 1 (PartitionOffset 2)] >>= (@?= Just (KafkaResponseError RdKafkaRespErrState))
            record consumer topic 0 2,
          scenario "callback-buffer-paused-partition-does-not-block-another" $ \topic consumer -> do
            assign consumer [part topic 0 (PartitionOffset 0), part topic 1 (PartitionOffset 0)] >>= (@?= Nothing)
            forM_ [1 .. if mode == CallbackPollModeSync then 6 else 1 :: Int] $ \_ -> pollConsumerEvents consumer (Just (Timeout 1000))
            pausePartitions consumer [(topic, PartitionId 0), (unknown topic, PartitionId 42)] >>= (@?= KafkaResponseError RdKafkaRespErrUnknownPartition)
            forM_ [0 .. 2] (record consumer topic 1)
            pollMessage consumer (Timeout 100) >>= (@?= Left (KafkaResponseError RdKafkaRespErrTimedOut))
            resumePartitions consumer [(unknown topic, PartitionId 42), (topic, PartitionId 0)] >>= (@?= KafkaResponseError RdKafkaRespErrUnknownPartition)
            forM_ [0 .. 2] (record consumer topic 0),
          scenario "callback-buffer-unassignment-discards-pending-records" $ \topic consumer -> do
            assign consumer [part topic 0 (PartitionOffset 0)] >>= (@?= Nothing)
            pollConsumerEvents consumer (Just (Timeout 1000))
            assign consumer [] >>= (@?= Nothing)
            pollMessage consumer (Timeout 100) >>= (@?= Left (KafkaResponseError RdKafkaRespErrTimedOut)),
          scenario "pausing-one-partition-leaves-the-other-running" $ \topic consumer -> do
            assign consumer [part topic 0 (PartitionOffset 0), part topic 1 (PartitionOffset 0)] >>= (@?= Nothing)
            forM_ [1 .. 2 :: Int] $ \_ -> pausePartitions consumer [(topic, PartitionId 0)] >>= (@?= KafkaResponseError RdKafkaRespErrNoError)
            forM_ [0 .. 2] (record consumer topic 1)
            pollMessage consumer (Timeout 100) >>= (@?= Left (KafkaResponseError RdKafkaRespErrTimedOut))
            forM_ [1 .. 2 :: Int] $ \_ -> resumePartitions consumer [(topic, PartitionId 0)] >>= (@?= KafkaResponseError RdKafkaRespErrNoError)
            forM_ [0 .. 2] (record consumer topic 0),
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
      <> bufferFieldTests env
      <> [cooperativeBufferTest env, mixedBufferTest env]

bufferFieldTests :: TestEnv -> [TestTree]
bufferFieldTests env =
  [ testCase ("sync-buffer-fields-" <> label) $
      withTopic env (Text.pack ("buffer-fields-" <> label)) 1 $ \topic producer -> do
        let fields = [(Just "\0\255key", Just "\255\0value", headersFromList [("same", ""), ("same", "\0\255")]), (Nothing, Nothing, mempty), (Just "", Just "", mempty)]
        forM_ fields $ \(key, value, headers) -> P.produceMessage producer (P.ProducerRecord topic (P.SpecifiedPartition 0) key value headers) >>= (@?= Nothing)
        P.flushProducerTimeout producer (Timeout 5000) >>= (@?= Nothing)
        let props = brokersList (teBrokers env) <> groupId (makeGroupId env (Text.pack ("buffer-fields-" <> label))) <> noAutoCommit <> callbackPollMode CallbackPollModeSync <> extraProp "enable.partition.eof" "true"
        bracket (newAssignedConsumer props >>= either throwIO pure) releaseConsumer $ \consumer -> do
          assign consumer [part topic 0 (PartitionOffset 0)] >>= (@?= Nothing)
          forM_ [1 .. 3 :: Int] $ \_ -> withMessageFault fault $ do
            pollConsumerEvents consumer (Just (Timeout 3000))
            mapM messageStat [1, 2, 5] >>= (@?= [1, 1, 0])
          -- EOF is retained with its origin; an empty poll must not add a timeout.
          pollConsumerEvents consumer (Just (Timeout 1000))
          pollConsumerEvents consumer (Just (Timeout 100))
          forM_ (zip [0 ..] fields) $ \(offset, (key, value, headers)) -> do
            message <- if offset == 1 then pollMessageAttributed consumer (Timeout 0) >>= either (assertFailure . show) pure else pollMessage consumer (Timeout 0) >>= either throwIO pure
            (crTopic message, crPartition message, crOffset message, crKey message, crValue message, crHeaders message) @?= (topic, PartitionId 0, Offset offset, key, value, headers)
            case timestamp of
              Just expected -> crTimestamp message @?= expected
              Nothing -> case crTimestamp message of
                CreateTime (Millis time) -> assertBool "timestamp is missing" (time > 0)
                other -> assertFailure (show other)
          pollMessageAttributed consumer (Timeout 0) >>= (@?= Left (ConsumerMessageError (KafkaResponseError RdKafkaRespErrPartitionEof) (Just topic) (Just (PartitionId 0)) (Just (Offset 3))))
          pollMessage consumer (Timeout 50) >>= (@?= Left (KafkaResponseError RdKafkaRespErrTimedOut))
  | (label, fault, timestamp) <- [("native-time", 16, Nothing), ("no-time", 17, Just NoTimestamp), ("append-time", 18, Just (LogAppendTime (Millis 123456789))), ("create-time", 19, Just (CreateTime (Millis 987654321)))]
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

-- Installed callback frames exercise exact subsets while all native protocol,
-- assignment, queue and incremental actions remain real.
cooperativeBufferTest :: TestEnv -> TestTree
cooperativeBufferTest env = testCase "cooperative subset callbacks preserve unrelated buffers pauses and originless errors" $
  withTopic env "cooperative-buffer-subset" 2 $ \topic producer -> do
    forM_ [0, 1] $ \partition -> forM_ [0 .. 2] $ \offset -> produceTo producer topic partition (payload partition offset)
    let props = consumerPropsNoStore env <> groupId (makeGroupId env "cooperative-buffer-subset") <> callbackPollMode CallbackPollModeSync <> extraProp "partition.assignment.strategy" "cooperative-sticky"
    withStateFault 9 $ bracket (newConsumer props (topics [topic] <> offsetReset Earliest) >>= either throwIO pure) releaseConsumer $ \consumer -> do
      timeout 10000000 (withMessageFault 16 (collect consumer 6)) >>= maybe (assertFailure "six callback records were not copied") pure
      fmap (M.map sort) <$> assignment consumer >>= (@?= Right (M.singleton topic [PartitionId 0, PartitionId 1]))
      produceTo producer topic 0 "originless-error-frame"
      withMessageFault 24 $ do
        pollConsumerEvents consumer (Just (Timeout 10000))
        messageStat 1 >>= (@?= 1)
        messageStat 2 >>= (@?= 1)
      pausePartitions consumer [(topic, PartitionId 0), (topic, PartitionId 1)] >>= (@?= KafkaResponseError RdKafkaRespErrNoError)
      triggerCooperative topic True
      fmap (M.map sort) <$> assignment consumer >>= (@?= Right (M.singleton topic [PartitionId 1]))
      -- Partition 1 remains paused across both subset actions. Partition 0's
      -- copied records must be discarded. The controlled re-assignment starts
      -- at END, so no copied record from partition 0 may reappear.
      triggerCooperative topic False
      fmap (M.map sort) <$> assignment consumer >>= (@?= Right (M.singleton topic [PartitionId 0, PartitionId 1]))
      resumePartitions consumer [(topic, PartitionId 0)] >>= (@?= KafkaResponseError RdKafkaRespErrNoError)
      pollMessageAttributed consumer (Timeout 0)
        >>= (@?= Left (ConsumerMessageError (KafkaResponseError RdKafkaRespErrBadMsg) Nothing Nothing Nothing))
      pollMessage consumer (Timeout 100) >>= (@?= Left (KafkaResponseError RdKafkaRespErrTimedOut))
      resumePartitions consumer [(topic, PartitionId 1)] >>= (@?= KafkaResponseError RdKafkaRespErrNoError)
      forM_ [0 .. 2] (record consumer topic 1)
      pollMessage consumer (Timeout 100) >>= (@?= Left (KafkaResponseError RdKafkaRespErrTimedOut))
  where
    collect consumer count = do
      pollConsumerEvents consumer (Just (Timeout 1000))
      acquired <- messageStat 1
      if acquired == count then messageStat 2 >>= (@?= count) else collect consumer count

mixedBufferTest :: TestEnv -> TestTree
mixedBufferTest env = testCase "sync callback mixed seek retains only the failed partition's copied records" $
  withTopic env "mixed-seek-buffer" 2 $ \topic producer -> do
    forM_ [0, 1] $ \partition -> forM_ [0 .. 2] $ \offset -> produceTo producer topic partition (payload partition offset)
    let props = consumerPropsNoStore env <> groupId (makeGroupId env "mixed-seek-buffer") <> callbackPollMode CallbackPollModeSync
    bracket (newAssignedConsumer props >>= either throwIO pure) releaseConsumer $ \consumer -> do
      assign consumer [part topic 0 (PartitionOffset 0), part topic 1 (PartitionOffset 0)] >>= (@?= Nothing)
      timeout 10000000 (withMessageFault 16 (collect consumer)) >>= maybe (assertFailure "six callback records were not copied") pure
      seekPartitions consumer [part topic 0 PartitionOffsetStored, part topic 1 (PartitionOffset 2)] (Timeout 2000)
        >>= (@?= Just (KafkaResponseError RdKafkaRespErrInvalidArg))
      records <- receiveMessages consumer 4 >>= either throwIO pure
      sort [(crTopic r, crPartition r, crOffset r, crValue r) | r <- records]
        @?= [(topic, PartitionId partition, Offset offset, Just (payload partition (fromIntegral offset))) | (partition, offsets) <- [(0, [0, 1, 2]), (1, [2])], offset <- offsets]
      pollMessage consumer (Timeout 100) >>= (@?= Left (KafkaResponseError RdKafkaRespErrTimedOut))
  where
    collect consumer = do
      pollConsumerEvents consumer (Just (Timeout 1000))
      acquired <- messageStat 1
      if acquired == 6 then messageStat 2 >>= (@?= 6) else collect consumer
