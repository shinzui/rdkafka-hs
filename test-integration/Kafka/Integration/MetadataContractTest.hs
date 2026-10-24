module Kafka.Integration.MetadataContractTest (tests, runCancellationProbe) where

import Control.Concurrent (forkFinally, forkIOWithUnmask, killThread, threadDelay)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, readMVar)
import Control.Exception (AsyncException (ThreadKilled), SomeException, bracket, finally, fromException, throwIO, try)
import Control.Monad (forM, forM_, replicateM_)
import Data.ByteString qualified as BS
import Data.List (sort)
import Data.Text qualified as Text
import GHC.Conc (BlockReason (BlockedOnException), ThreadStatus (ThreadBlocked), threadStatus)
import Kafka.CallbackTestSupport (runChildProbe)
import Kafka.Consumer
import Kafka.Metadata
import Kafka.MetadataTestSupport
import Kafka.Producer qualified as P
import Kafka.TestEnv
import System.Environment (lookupEnv)
import System.Exit (die)
import System.Timeout (timeout)
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertBool, assertFailure, testCase, (@?=))

tests :: TestEnv -> TestTree
tests env =
  testGroup "metadata-contract" $
    [ testCase "native metadata counter detects an unreleased result" $ withMetadataFault 0 $ do
        metadataLeakControl (teBrokers env)
        mapM metadataStat [0, 1] >>= (@?= [1, 0]),
      testCase "topic identity replica sets and exact watermarks" $
        withTopic env "metadata-exact" 2 $ \topic producer -> do
          replicateM_ 3 (produceTo producer topic 0 "partition-zero")
          produceTo producer topic 1 "partition-one"
          withMetadataFault 0 $ do
            metadata <- topicMetadata producer (Timeout 5000) topic >>= either throwIO pure
            map tmTopicName (kmTopics metadata) @?= [topic]
            let brokers = map bmBrokerId (kmBrokers metadata)
            assertBool (show metadata) (kmOrigBroker metadata `elem` brokers)
            case kmTopics metadata of
              [tm] -> do
                tmError tm @?= Nothing
                sort (map pmPartitionId (tmPartitions tm)) @?= [PartitionId 0, PartitionId 1]
                forM_ (tmPartitions tm) $ \partition -> do
                  pmError partition @?= Nothing
                  assertBool (show partition) (pmLeader partition `elem` brokers)
                  pmReplicas partition @?= [pmLeader partition]
                  pmInSyncReplicas partition @?= [pmLeader partition]
                let expected = Right [WatermarkOffsets topic (PartitionId 0) (Offset 0) (Offset 3), WatermarkOffsets topic (PartitionId 1) (Offset 0) (Offset 1)]
                sequence <$> watermarkOffsets producer (Timeout 5000) topic >>= (@?= expected)
                sequence <$> watermarkOffsets' producer (Timeout 5000) tm >>= (@?= expected)
              other -> assertFailure (show other)
            allMetadata <- allTopicsMetadata producer (Timeout 5000) >>= either throwIO pure
            map tmTopicName (filter ((== topic) . tmTopicName) (kmTopics allMetadata)) @?= [topic]
            created <- metadataStat 0
            destroyed <- metadataStat 1
            (created, destroyed) @?= (3, 3),
      testCase "provided metadata preserves partition failures in convenience helpers" $
        withTopic env "metadata-provided-errors" 2 $ \topic producer -> do
          produceTo producer topic 0 "zero"
          produceTo producer topic 1 "one"
          actual <- topicMetadata producer (Timeout 5000) topic >>= either throwIO pure
          case kmTopics actual of
            [metadata] -> do
              let failed = metadata {tmPartitions = [partition {pmError = if pmPartitionId partition == PartitionId 0 then Just partitionFailure else Nothing} | partition <- tmPartitions metadata]}
              watermarkOffsets' producer (Timeout 5000) failed >>= (@?= [Left partitionFailure, Right (WatermarkOffsets topic (PartitionId 1) (Offset 0) (Offset 1))])
              offsetsForTime' producer (Timeout 5000) (Millis 0) failed >>= (@?= Left partitionFailure)
            other -> assertFailure (show other),
      testCase "native metadata partition failures survive convenience helpers" $
        withTopic env "metadata-native-errors" 2 $ \topic producer -> do
          produceTo producer topic 0 "zero"
          produceTo producer topic 1 "one"
          withMetadataFault 3 $ do
            metadata <- topicMetadata producer (Timeout 5000) topic >>= either throwIO pure
            map (map pmError . tmPartitions) (kmTopics metadata) @?= [[Just partitionFailure, Nothing]]
            watermarkOffsets producer (Timeout 5000) topic >>= (@?= [Left partitionFailure, Right (WatermarkOffsets topic (PartitionId 1) (Offset 0) (Offset 1))])
            topicOffsetsForTime producer (Timeout 5000) (Millis 0) topic >>= (@?= Left partitionFailure)
            mapM metadataStat [0, 1] >>= (@?= [3, 3]),
      testCase "group query timeout preserves initial metadata wait failure" $
        bracket (P.newProducer (P.brokersList ["127.0.0.1:1"] <> P.suppressDisconnectLogs) >>= either throwIO pure) releaseProducer $ \producer -> do
          allConsumerGroupsInfo producer (Timeout 200) >>= (@?= Left (KafkaResponseError RdKafkaRespErrTimedOut))
          consumerGroupInfo producer (Timeout 200) (makeGroupId env "unreachable-group") >>= (@?= Left (KafkaResponseError RdKafkaRespErrTimedOut)),
      testCase "missing topic errors survive convenience helpers" $
        withClient env "missing-topic" $ \consumer -> do
          let absent = TopicName (prefixedName env "metadata-absent")
          metadata <- topicMetadata consumer (Timeout 5000) absent >>= either throwIO pure
          map (\topic -> (tmTopicName topic, tmError topic, tmPartitions topic)) (kmTopics metadata)
            @?= [(absent, Just unknownTopic, [])]
          watermarkOffsets consumer (Timeout 5000) absent >>= (@?= [Left unknownTopic])
          topicOffsetsForTime consumer (Timeout 5000) (Millis 0) absent >>= (@?= Left unknownTopic)
          let failed = TopicMetadata absent [] (Just unknownTopic)
          watermarkOffsets' consumer (Timeout 5000) failed >>= (@?= [Left unknownTopic])
          offsetsForTime' consumer (Timeout 5000) (Millis 0) failed >>= (@?= Left unknownTopic),
      testCase "timestamp lookup distinguishes before at and after actual record times" $
        withTopic env "metadata-times" 1 $ \topic producer -> do
          forM_ ["one", "two", "three"] $ \value -> produceTo producer topic 0 value >> threadDelay 5000
          withClient env "timestamps" $ \consumer -> do
            assign consumer [TopicPartition topic (PartitionId 0) (PartitionOffset 0)] >>= (@?= Nothing)
            records <- forM [0 .. 2] $ \offset -> do
              message <- pollMessage consumer (Timeout 10000) >>= either throwIO pure
              crOffset message @?= Offset offset
              case crTimestamp message of
                CreateTime (Millis time) -> pure time
                other -> assertFailure (show other)
            case records of
              [first, second, third] -> do
                assertBool (show records) (first < second && second < third)
                forM_ [(first - 1, PartitionOffset 0), (first, PartitionOffset 0), (second, PartitionOffset 1), (third, PartitionOffset 2), (third + 1, PartitionOffsetEnd)] $ \(time, expected) -> do
                  let result = Right [TopicPartition topic (PartitionId 0) expected]
                  offsetsForTime consumer (Timeout 5000) (Millis time) [(topic, PartitionId 0), (topic, PartitionId 0)] >>= (@?= result)
                  topicOffsetsForTime consumer (Timeout 5000) (Millis time) topic >>= (@?= result)
                metadata <- topicMetadata consumer (Timeout 5000) topic >>= either throwIO pure
                case kmTopics metadata of
                  [tm] -> offsetsForTime' consumer (Timeout 5000) (Millis second) tm >>= (@?= Right [TopicPartition topic (PartitionId 0) (PartitionOffset 1)])
                  other -> assertFailure (show other)
              other -> assertFailure (show other)
            offsetsForTime consumer (Timeout 100) (Millis 0) [] >>= (@?= Left (KafkaResponseError RdKafkaRespErrInvalidArg)),
      testCase "mixed timestamp request preserves native partition failure" $
        withTopic env "metadata-mixed" 1 $ \topic producer -> do
          produceTo producer topic 0 "record"
          withClient env "mixed-time" $ \consumer -> do
            result <- offsetsForTime consumer (Timeout 5000) (Millis 0) [(topic, PartitionId 0), (topic, PartitionId 42)]
            result @?= Left (KafkaResponseError RdKafkaRespErrUnknownPartition),
      testCase "metadata timeouts preserve the native error" $
        bracket (P.newProducer (P.brokersList ["127.0.0.1:1"] <> P.extraProp "log_level" "0") >>= either throwIO pure) releaseProducer $ \producer -> do
          allTopicsMetadata producer (Timeout 100) >>= (@?= Left (KafkaResponseError RdKafkaRespErrTransport))
          topicMetadata producer (Timeout 100) (TopicName (prefixedName env "metadata-timeout")) >>= (@?= Left (KafkaResponseError RdKafkaRespErrTransport))
    ]
      <> [ testCase (label <> " native acquisition cancellation releases its result") $
             runChildProbe ("metadata-" <> label) ["--metadata-cancellation-probe", label]
         | label <- ["metadata", "groups"]
         ]
      <> [ testCase (label <> " group identity lifecycle and supported native states") $
             withTopic env (Text.pack ("metadata-group-" <> label)) 1 $ \topic producer -> do
               produceTo producer topic 0 "group-record"
               let group = makeGroupId env (Text.pack ("metadata-group-λ-配置-" <> label))
                   props = brokersList (teBrokers env) <> groupId group <> clientId (ClientId (Text.pack ("metadata-client-λ-配置-" <> label))) <> noAutoCommit <> callbackPollMode mode
               bracket (newConsumer props (topics [topic] <> offsetReset Earliest) >>= either throwIO pure) releaseConsumer $ \consumer -> do
                 message <- pollMessage consumer (Timeout 10000) >>= either throwIO pure
                 crValue message @?= Just "group-record"
                 withMetadataFault 0 $ do
                   described <- consumerGroupInfo producer (Timeout 5000) group >>= either throwIO pure
                   map (\info -> (giGroup info, giError info, giState info, giProtocolType info)) described @?= [(group, Nothing, GroupStable, GroupProtocolType "consumer")]
                   case described of
                     [info] -> do
                       map gmiClientId (giMembers info) @?= [ClientId (Text.pack ("metadata-client-λ-配置-" <> label))]
                       assertBool (show info) (not (Text.null (let GroupProtocol protocol = giProtocol info in protocol)))
                     other -> assertFailure (show other)
                   allGroups <- allConsumerGroupsInfo producer (Timeout 5000) >>= either throwIO pure
                   filter ((== group) . giGroup) allGroups @?= described
                   mapM metadataStat [2, 3] >>= (@?= [2, 2])
                 forM_ [(10, GroupPreparingRebalance), (11, GroupAwaitingSync), (12, GroupAwaitingSync), (13, GroupStable), (14, GroupDead), (15, GroupEmpty)] $ \(fault, expected) ->
                   withMetadataFault fault $ do
                     result <- consumerGroupInfo producer (Timeout 5000) group
                     fmap (map giState) result @?= Right [expected]
                     mapM metadataStat [2, 3] >>= (@?= [1, 1])
                 withMetadataFault 16 $ do
                   result <- consumerGroupInfo producer (Timeout 5000) group
                   result @?= Left (KafkaError "Unsupported Kafka consumer group state: FutureState")
                   mapM metadataStat [2, 3] >>= (@?= [1, 1])
                 withMetadataFault 18 $ do
                   described <- consumerGroupInfo producer (Timeout 5000) group >>= either throwIO pure
                   map giError described @?= [Just (KafkaResponseError RdKafkaRespErrGroupAuthorizationFailed)]
                   mapM metadataStat [2, 3] >>= (@?= [1, 1])
                 withMetadataFault 17 $ do
                   described <- consumerGroupInfo producer (Timeout 5000) group >>= either throwIO pure
                   map giMembers described @?= [[GroupMemberInfo (GroupMemberId "member-field-λ-配置") (ClientId "client-field-λ-配置") "host-field-λ-配置" (BS.pack [0, 1, 0, 255]) (BS.pack [255, 0, 2, 0, 3])]]
                   mapM metadataStat [2, 3] >>= (@?= [1, 1])
               described <- consumerGroupInfo producer (Timeout 5000) group >>= either throwIO pure
               map giState described @?= [GroupEmpty]
               consumerGroupInfo producer (Timeout 5000) (makeGroupId env (Text.pack ("metadata-nonexistent-" <> label))) >>= (@?= Right [])
         | (label, mode) <- [("async", CallbackPollModeAsync), ("sync", CallbackPollModeSync)]
         ]

partitionFailure :: KafkaError
partitionFailure = KafkaResponseError RdKafkaRespErrTopicAuthorizationFailed

unknownTopic :: KafkaError
unknownTopic = KafkaResponseError RdKafkaRespErrUnknownTopicOrPart

withClient :: TestEnv -> Text.Text -> (KafkaConsumer -> IO a) -> IO a
withClient env label = bracket (newAssignedConsumer (brokersList (teBrokers env) <> groupId (makeGroupId env ("metadata-" <> label)) <> noAutoCommit <> callbackPollMode CallbackPollModeSync <> extraProp "allow.auto.create.topics" "false") >>= either throwIO pure) releaseConsumer

runCancellationProbe :: String -> IO ()
runCancellationProbe label = do
  brokers <- lookupEnv "RDKAFKA_TEST_BROKER" >>= either die pure . resolveBrokers
  env <- newTestEnv brokers
  withTopic env "metadata-cancel" 1 $ \topic producer -> case label of
    "metadata" -> withMetadataFault 1 (cancelled [1, 1, 0, 0] (topicMetadata producer (Timeout 5000) topic))
    "groups" -> withMetadataFault 2 (cancelled [0, 0, 1, 1] (consumerGroupInfo producer (Timeout 5000) (makeGroupId env "metadata-cancel-absent")))
    _ -> die "invalid metadata cancellation probe"

cancelled :: [Int] -> IO a -> IO ()
cancelled expected action = do
  done <- newEmptyMVar
  worker <- forkIOWithUnmask $ \unmask -> try @SomeException (unmask action) >>= putMVar done
  let bounded operation = timeout 10000000 operation >>= maybe (assertFailure "metadata worker timed out") pure
  flip finally (releaseMetadataBoundary >> bounded (readMVar done)) $ do
    awaitMetadataBoundary
    killed <- newEmptyMVar
    killer <- forkFinally (killThread worker) (putMVar killed)
    let pending = threadStatus killer >>= \status -> if status == ThreadBlocked BlockedOnException then pure () else threadDelay 1000 >> pending
    bounded pending
    releaseMetadataBoundary
    bounded (readMVar done) >>= \case
      Left exception -> (fromException exception :: Maybe AsyncException) @?= Just ThreadKilled
      Right _ -> assertFailure "metadata cancellation was not delivered"
    bounded (readMVar killed) >>= either throwIO pure
    mapM metadataStat [0 .. 3] >>= (@?= expected)
