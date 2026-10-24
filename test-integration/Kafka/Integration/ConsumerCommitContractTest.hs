module Kafka.Integration.ConsumerCommitContractTest (tests) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.MVar (MVar, newEmptyMVar, tryPutMVar, tryTakeMVar)
import Control.Exception (bracket, throwIO)
import Control.Monad (void)
import Data.Text qualified as Text
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
