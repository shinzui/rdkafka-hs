module Kafka.Integration.ConsumerCommitContractTest (tests) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.MVar (MVar, newEmptyMVar, tryPutMVar, tryTakeMVar)
import Control.Exception (bracket, throwIO)
import Control.Monad (forM_, void)
import Data.List (find, sort)
import Data.Text qualified as Text
import Data.Text.Encoding qualified as TE
import Kafka.Consumer
import Kafka.ConsumerStateSupport (commitObservation, withStateFault)
import Kafka.TestEnv
import System.Timeout (timeout)
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertFailure, testCase, (@?=))

tests :: TestEnv -> TestTree
tests env =
  testGroup "consumer-commit" $
    [ testCase (label <> " commit modes preserve native flags callback offsets and broker state") $
        withTopic env (Text.pack ("commit-modes-" <> label)) 1 $ \topic producer -> do
          produceTo producer topic 0 "commit-record"
          events <- newEmptyMVar
          let props = brokersList (teBrokers env) <> groupId (makeGroupId env (Text.pack ("commit-modes-" <> label))) <> noAutoCommit <> noAutoOffsetStore <> callbackPollMode mode <> setCallback (offsetCommitCallback (\_ reportedError partitions -> void (tryPutMVar events (reportedError, partitions))))
              part offset = TopicPartition topic (PartitionId 0) (PartitionOffset offset)
          withStateFault 0 $
            bracket (newAssignedConsumer props >>= either throwIO pure) releaseConsumer $ \consumer -> do
              assign consumer [part 0] >>= (@?= Nothing)
              record <- pollMessage consumer (Timeout 10000) >>= either throwIO pure
              (crTopic record, crOffset record, crValue record) @?= (topic, Offset 0, Just "commit-record")
              commitOffsetMessage OffsetCommitAsync consumer record >>= (@?= Nothing)
              commitObservation >>= (@?= (1, 1))
              awaitCommit consumer events >>= (@?= (KafkaResponseError RdKafkaRespErrNoError, [part 1]))
              committed consumer (Timeout 5000) [(topic, PartitionId 0)] >>= (@?= Right [part 1])
              commitPartitionsOffsets OffsetCommitAsync consumer [part 0] >>= (@?= Nothing)
              commitObservation >>= (@?= (2, 1))
              awaitCommit consumer events >>= (@?= (KafkaResponseError RdKafkaRespErrNoError, [part 0]))
              committed consumer (Timeout 5000) [(topic, PartitionId 0)] >>= (@?= Right [part 0])
              storeOffsets consumer [part 1] >>= (@?= Nothing)
              commitAllOffsets OffsetCommitAsync consumer >>= (@?= Nothing)
              commitObservation >>= (@?= (3, 1))
              awaitCommit consumer events >>= (@?= (KafkaResponseError RdKafkaRespErrNoError, [part 1]))
              committed consumer (Timeout 5000) [(topic, PartitionId 0)] >>= (@?= Right [part 1])
              commitPartitionsOffsets OffsetCommit consumer [part 2] >>= (@?= Nothing)
              commitObservation >>= (@?= (4, 0))
              awaitCommit consumer events >>= (@?= (KafkaResponseError RdKafkaRespErrNoError, [part 2]))
              committed consumer (Timeout 5000) [(topic, PartitionId 0)] >>= (@?= Right [part 2])
    | (label, mode) <- [("async", CallbackPollModeAsync), ("sync", CallbackPollModeSync)]
    ]
      <> multiPartitionCommitTests env
      <> emptyCommitTests env
      <> mixedCommitTests env
      <> timeoutCommitTests env

emptyCommitTests :: TestEnv -> [TestTree]
emptyCommitTests env =
  [ testCase (label <> " empty commit requests preserve no-offset callbacks") $
      withTopic env (Text.pack ("commit-empty-" <> label)) 1 $ \_ _ -> do
        events <- newEmptyMVar
        let props = brokersList (teBrokers env) <> groupId (makeGroupId env (Text.pack ("commit-empty-" <> label))) <> noAutoCommit <> noAutoOffsetStore <> callbackPollMode mode <> setCallback (offsetCommitCallback (\_ reportedError partitions -> void (tryPutMVar events (reportedError, partitions))))
            noOffset = KafkaResponseError RdKafkaRespErrNoOffset
        withStateFault 0 $
          bracket (newAssignedConsumer props >>= either throwIO pure) releaseConsumer $ \consumer -> do
            committed consumer (Timeout 5000) [] >>= (@?= Right [])
            commitPartitionsOffsets OffsetCommit consumer [] >>= (@?= Just noOffset)
            awaitCommit consumer events >>= (@?= (noOffset, []))
            commitAllOffsets OffsetCommit consumer >>= (@?= Just noOffset)
            awaitCommit consumer events >>= (@?= (noOffset, []))
            commitPartitionsOffsets OffsetCommitAsync consumer [] >>= (@?= Nothing)
            commitObservation >>= (@?= (3, 1))
            awaitCommit consumer events >>= (@?= (noOffset, []))
  | (label, mode) <- [("async", CallbackPollModeAsync), ("sync", CallbackPollModeSync)]
  ]

mixedCommitTests :: TestEnv -> [TestTree]
mixedCommitTests env =
  [ testCase (label <> " mixed broker commit failures preserve successful offsets") $
      withTopic env (Text.pack ("commit-mixed-" <> label)) 1 $ \topic producer -> do
        produceTo producer topic 0 "commit-record"
        events <- newEmptyMVar
        let props = brokersList (teBrokers env) <> groupId (makeGroupId env (Text.pack ("commit-mixed-" <> label))) <> noAutoCommit <> noAutoOffsetStore <> callbackPollMode mode <> setCallback (offsetCommitCallback (\_ reportedError partitions -> void (tryPutMVar events (reportedError, partitions))))
            part partition offset = TopicPartition topic (PartitionId partition) (PartitionOffset offset)
            failure = KafkaResponseError RdKafkaRespErrUnknownTopicOrPart
        bracket (newAssignedConsumer props >>= either throwIO pure) releaseConsumer $ \consumer -> do
          assign consumer [part 0 0] >>= (@?= Nothing)
          _ <- pollMessage consumer (Timeout 10000) >>= either throwIO pure
          commitPartitionsOffsets OffsetCommit consumer [part 0 1, part 42 9] >>= (@?= Just failure)
          awaitCommit consumer events >>= (@?= (failure, [part 0 1, part 42 9]))
          committed consumer (Timeout 5000) [(topic, PartitionId 0)] >>= (@?= Right [part 0 1])
          committed consumer (Timeout 5000) [(topic, PartitionId 0), (topic, PartitionId 42)] >>= (@?= Right [part 0 1, TopicPartition topic (PartitionId 42) PartitionOffsetInvalid])
  | (label, mode) <- [("async", CallbackPollModeAsync), ("sync", CallbackPollModeSync)]
  ]

timeoutCommitTests :: TestEnv -> [TestTree]
timeoutCommitTests env =
  [ testCase (label <> " committed query preserves an unavailable coordinator timeout") $
      bracket (newAssignedConsumer (brokersList ["127.0.0.1:1"] <> groupId (makeGroupId env (Text.pack ("commit-timeout-" <> label))) <> noAutoCommit <> callbackPollMode mode <> extraProp "log_level" "0") >>= either throwIO pure) releaseConsumer $ \consumer -> do
        committed consumer (Timeout 200) [(TopicName (prefixedName env "commit-timeout"), PartitionId 0)] >>= (@?= Left (KafkaResponseError RdKafkaRespErrTimedOut))
  | (label, mode) <- [("async", CallbackPollModeAsync), ("sync", CallbackPollModeSync)]
  ]

awaitCommit :: KafkaConsumer -> MVar (KafkaError, [TopicPartition]) -> IO (KafkaError, [TopicPartition])
awaitCommit consumer events = timeout 10000000 loop >>= maybe (assertFailure "commit callback timed out") pure
  where
    loop =
      tryTakeMVar events >>= \case
        Just result -> pure result
        Nothing -> do
          pollMessage consumer (Timeout 50) >>= \case
            Left (KafkaResponseError RdKafkaRespErrTimedOut) -> pure ()
            Left failure -> throwIO failure
            Right record -> assertFailure ("unexpected extra record while awaiting callback: " <> show record)
          threadDelay 1000
          loop

multiPartitionCommitTests :: TestEnv -> [TestTree]
multiPartitionCommitTests env =
  [ testCase (label <> " message and stored commits preserve unrelated partition offsets") $
      withTopic env (Text.pack ("commit-multiple-" <> label)) 2 $ \topic producer -> do
        forM_ [0, 1] $ \partition -> forM_ [0 .. 2 :: Int] $ \offset -> produceTo producer topic partition (TE.encodeUtf8 (Text.pack (show (partition, offset))))
        events <- newEmptyMVar
        let props = brokersList (teBrokers env) <> groupId (makeGroupId env (Text.pack ("commit-multiple-" <> label))) <> noAutoCommit <> noAutoOffsetStore <> callbackPollMode mode <> setCallback (offsetCommitCallback (\_ err partitions -> void (tryPutMVar events (err, partitions))))
            part partition offset = TopicPartition topic (PartitionId partition) (PartitionOffset offset)
            report consumer expected = awaitCommit consumer events >>= (@?= (KafkaResponseError RdKafkaRespErrNoError, expected))
            query consumer expected = committed consumer (Timeout 5000) [(topic, PartitionId 0), (topic, PartitionId 1)] >>= (@?= Right expected)
        bracket (newAssignedConsumer props >>= either throwIO pure) releaseConsumer $ \consumer -> do
          assign consumer [part 0 0, part 1 0] >>= (@?= Nothing)
          records <- receiveMessages consumer 6 >>= either throwIO pure
          sort [(crPartition r, crOffset r) | r <- records] @?= [(PartitionId p, Offset n) | p <- [0, 1], n <- [0 .. 2]]
          let record p n = maybe (assertFailure "expected source record is absent") pure (find (\r -> crPartition r == PartitionId p && crOffset r == Offset n) records)
          record 0 0 >>= commitOffsetMessage OffsetCommit consumer >>= (@?= Nothing)
          report consumer [part 0 1]
          query consumer [part 0 1, TopicPartition topic (PartitionId 1) PartitionOffsetInvalid]
          record 1 1 >>= commitOffsetMessage OffsetCommitAsync consumer >>= (@?= Nothing)
          report consumer [part 1 2]
          query consumer [part 0 1, part 1 2]
          record 0 2 >>= storeOffsetMessage consumer >>= (@?= Nothing)
          commitAllOffsets OffsetCommit consumer >>= (@?= Nothing)
          report consumer [part 0 3, TopicPartition topic (PartitionId 1) PartitionOffsetInvalid]
          query consumer [part 0 3, part 1 2]
          record 1 0 >>= storeOffsetMessage consumer >>= (@?= Nothing)
          commitAllOffsets OffsetCommit consumer >>= (@?= Just (KafkaResponseError RdKafkaRespErrNoOffset))
          awaitCommit consumer events >>= (@?= (KafkaResponseError RdKafkaRespErrNoOffset, [TopicPartition topic (PartitionId p) PartitionOffsetInvalid | p <- [0, 1]]))
          query consumer [part 0 3, part 1 2]
          record 1 2 >>= storeOffsetMessage consumer >>= (@?= Nothing)
          commitAllOffsets OffsetCommit consumer >>= (@?= Nothing)
          report consumer [TopicPartition topic (PartitionId 0) PartitionOffsetInvalid, part 1 3]
          query consumer [part 0 3, part 1 3]
          storeOffsets consumer [part 0 4, part 1 5] >>= (@?= Nothing)
          commitAllOffsets OffsetCommitAsync consumer >>= (@?= Nothing)
          report consumer [part 0 4, part 1 5]
          query consumer [part 0 4, part 1 5]
  | (label, mode) <- [("async", CallbackPollModeAsync), ("sync", CallbackPollModeSync)]
  ]
