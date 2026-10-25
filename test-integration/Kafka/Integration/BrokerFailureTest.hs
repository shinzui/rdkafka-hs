module Kafka.Integration.BrokerFailureTest (tests) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.MVar
import Control.Exception (bracket, finally, throwIO)
import Data.ByteString (ByteString)
import Data.Int (Int64)
import Data.List (sort)
import Data.Map.Strict qualified as M
import Data.Text qualified as T
import Kafka.Compatibility.KafkaFixture
import Kafka.Consumer qualified as C
import Kafka.Metadata qualified as D
import Kafka.Producer qualified as P
import Kafka.TestEnv
import Kafka.Topic qualified as A
import Kafka.Topic.Types
import Kafka.Types
import System.Timeout (timeout)
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit

bounded :: String -> IO a -> IO a
bounded label action = timeout 30000000 action >>= maybe (assertFailure label) pure

tests :: IO KafkaFixture -> TestTree
tests getFixture =
  testGroup
    "broker-failure"
    [ testCase (label <> " observed leader election preserves exact records and commits") $ do
        fixture <- getFixture
        let environment = fixtureEnv fixture
            topic = TopicName (prefixedName environment ("leader-" <> T.pack label))
            properties = P.brokersList (teBrokers environment) <> P.extraProps (M.fromList [("enable.idempotence", "true"), ("acks", "all"), ("message.timeout.ms", "15000")])
        bracket (P.newProducer properties >>= either throwIO pure) releaseProducer $ \producer -> do
          A.createTopic producer (NewTopic topic (PartitionCount 1) (ReplicationFactor 3) (M.singleton "min.insync.replicas" "2")) >>= (@?= Right topic)
          flip finally (A.deleteTopic producer topic >>= (@?= Right topic)) $ do
            let props = C.brokersList (teBrokers environment) <> C.groupId (makeGroupId environment ("leader-" <> T.pack label)) <> C.noAutoCommit <> C.noAutoOffsetStore <> C.callbackPollMode mode
            bracket (C.newAssignedConsumer props >>= either throwIO pure) releaseConsumer $ \consumer -> do
              C.assign consumer [C.TopicPartition topic (PartitionId 0) C.PartitionOffsetBeginning] >>= (@?= Nothing)
              sendExact producer topic 0
              first <- receiveExact consumer topic 0
              C.commitOffsetMessage C.OffsetCommit consumer first >>= (@?= Nothing)
              verifyCommit consumer topic 1
              before <- awaitMetadata producer topic (\p -> D.pmError p == Nothing && sort (D.pmInSyncReplicas p) == map BrokerId [1, 2, 3])
              sort (D.pmReplicas before) @?= map BrokerId [1, 2, 3]
              let old = D.pmLeader before
              assertBool (show before) (old `elem` map BrokerId [1, 2, 3])
              putStrLn ("[leader-before] " <> label <> " " <> show before)
              flip finally (startBroker fixture old >> awaitMetadata producer topic (\p -> D.pmError p == Nothing && sort (D.pmInSyncReplicas p) == map BrokerId [1, 2, 3])) $ do
                stopBroker fixture old
                changed <- awaitMetadata producer topic (\p -> D.pmError p == Nothing && D.pmLeader p /= old && D.pmLeader p `elem` filter (/= old) (map BrokerId [1, 2, 3]) && sort (D.pmInSyncReplicas p) == filter (/= old) (map BrokerId [1, 2, 3]))
                putStrLn ("[leader-elected] " <> label <> " " <> show changed)
                sendExact producer topic 1
                second <- receiveExact consumer topic 1
                C.commitOffsetMessage C.OffsetCommit consumer second >>= (@?= Nothing)
                verifyCommit consumer topic 2
                C.pollMessage consumer (Timeout 100) >>= (@?= Left (KafkaResponseError C.RdKafkaRespErrTimedOut))
                putStrLn ("[leader-recovered] " <> label <> " records=[0,1] committed=2")
    | (label, mode) <- [("async", C.CallbackPollModeAsync), ("sync", C.CallbackPollModeSync)]
    ]

sendExact :: P.KafkaProducer -> TopicName -> Int64 -> IO ()
sendExact producer topic offset = do
  report <- newEmptyMVar
  let record = P.ProducerRecord topic (P.SpecifiedPartition 0) (Just "leader-key") (Just (if offset == 0 then "before" else "after")) (headersFromList [("origin", "mp3")])
  P.produceMessage' producer record (putMVar report) >>= (@?= Right ())
  P.flushProducerTimeout producer (Timeout 15000) >>= (@?= Nothing)
  bounded "missing leader-change delivery report" (takeMVar report) >>= (@?= P.DeliverySuccess record (C.Offset offset))

receiveExact :: C.KafkaConsumer -> TopicName -> Int64 -> IO (C.ConsumerRecord (Maybe ByteString) (Maybe ByteString))
receiveExact consumer topic offset = bounded "missing leader-change record" go
  where
    go =
      C.pollMessage consumer (Timeout 1000) >>= \case
        Left (KafkaResponseError C.RdKafkaRespErrTimedOut) -> go
        Left err -> throwIO err
        Right record -> do
          (C.crTopic record, C.crPartition record, C.crOffset record, C.crKey record, C.crValue record, C.crHeaders record)
            @?= (topic, PartitionId 0, C.Offset offset, Just "leader-key", Just (if offset == 0 then "before" else "after"), headersFromList [("origin", "mp3")])
          pure record

verifyCommit :: C.KafkaConsumer -> TopicName -> Int64 -> IO ()
verifyCommit consumer topic offset = C.committed consumer (Timeout 5000) [(topic, PartitionId 0)] >>= (@?= Right [C.TopicPartition topic (PartitionId 0) (C.PartitionOffset offset)])

awaitMetadata :: P.KafkaProducer -> TopicName -> (D.PartitionMetadata -> Bool) -> IO D.PartitionMetadata
awaitMetadata producer topic predicate = bounded "owned leader/ISR transition was not observed" go
  where
    go = do
      result <- D.topicMetadata producer (Timeout 2000) topic >>= either throwIO pure
      case D.kmTopics result of
        [metadata] | D.tmError metadata == Nothing -> case D.tmPartitions metadata of
          [partition] | predicate partition -> pure partition
          _ -> threadDelay 100000 >> go
        _ -> assertFailure (show result)
