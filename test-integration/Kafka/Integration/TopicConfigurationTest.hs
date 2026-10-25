module Kafka.Integration.TopicConfigurationTest (tests) where

import Control.Concurrent (forkFinally, killThread, threadDelay)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, readMVar, takeMVar)
import Control.Exception (AsyncException (ThreadKilled), bracket, finally, fromException, throwIO)
import Control.Monad (forM_, unless, when)
import Data.List (sort)
import Data.Map qualified as Map
import Data.Text qualified as Text
import Foreign.C.Types (CInt)
import GHC.Clock (getMonotonicTime)
import GHC.Conc (BlockReason (BlockedOnException), ThreadStatus (ThreadBlocked), threadStatus)
import Kafka.AdminTestSupport
import Kafka.Consumer qualified as C
import Kafka.FaultTestSupport (withUnresponsiveBroker)
import Kafka.Metadata
import Kafka.Producer
import Kafka.TestEnv
import Kafka.Topic
import System.Environment (lookupEnv)
import System.Exit (ExitCode (..))
import System.Process (readProcessWithExitCode)
import System.Timeout (timeout)
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertBool, assertFailure, testCase, (@?=))

tests :: TestEnv -> TestTree
tests env =
  testGroup "topic-configuration" $
    [ testCase "singleton configuration reaches the broker" $
        bracket (mkProducer env) releaseProducer $ \producer -> withNativeTracking $ do
          let topic = TopicName (prefixedName env "config-singleton")
              create =
                createTopic producer (NewTopic topic (PartitionCount 1) (ReplicationFactor 1) (Map.singleton "cleanup.policy" "compact"))
                  >>= either throwIO pure
              remove name = deleteTopic producer name >>= (@?= Right name)
          bracket create remove $ \created -> do
            created @?= topic
            metadata <- topicMetadata producer (Timeout 5000) topic
            map (\t -> (tmTopicName t, map pmPartitionId (tmPartitions t))) . kmTopics <$> metadata
              @?= Right [(topic, [PartitionId 0])]
            values <- configValues topic "cleanup.policy"
            values @?= ["compact"]
          submissions >>= (@?= 1)
          counts NativeNewTopic >>= (@?= (1, 1))
          counts NativeDeleteTopic >>= (@?= (1, 1))
          mapM_ (\kind -> counts kind >>= \(allocated, _) -> assertBool ("unobserved " <> show kind) (allocated > 0)) [NativeQueue, NativeOptions, NativeEvent]
          assertBalanced,
      testCase "native allocation control detects a retained request" $
        withNativeTracking $ do
          withLeakControl $ counts NativeNewTopic >>= (@?= (1, 0))
          counts NativeNewTopic >>= (@?= (1, 1))
          assertBalanced,
      testCase "setter rejection releases allocated request before submission" $
        bracket (mkProducer env) releaseProducer $ \producer -> withNativeTracking $ do
          let topic = TopicName (prefixedName env "config-rejected-setter")
          failConfiguration True
          result <- createTopic producer (NewTopic topic (PartitionCount 1) (ReplicationFactor 1) (Map.singleton "cleanup.policy" "compact"))
          result @?= Left (KafkaResponseError RdKafkaRespErrInvalidArg)
          counts NativeNewTopic >>= (@?= (1, 1))
          counts NativeEvent >>= (@?= (0, 0))
          metadata <- allTopicsMetadata producer (Timeout 5000)
          (elem topic . map tmTopicName . kmTopics) <$> metadata @?= Right False
          submissions >>= (@?= 0)
          assertBalanced,
      testCase "interruption after allocation releases request before submission" $
        bracket (mkProducer env) releaseProducer $ \producer -> withNativeTracking $ do
          let topic = TopicName (prefixedName env "config-interrupted")
          blockConfiguration True
          completed <- newEmptyMVar
          worker <-
            forkFinally
              (createTopic producer (NewTopic topic (PartitionCount 1) (ReplicationFactor 1) (Map.singleton "cleanup.policy" "compact")))
              (putMVar completed)
          flip finally (blockConfiguration False >> killThread worker) $ do
            await configurationEntered
            counts NativeNewTopic >>= (@?= (1, 0))
            interrupted <- newEmptyMVar
            killer <- forkFinally (killThread worker) (putMVar interrupted)
            -- The killer is observed waiting for delivery while the native
            -- setter remains blocked. No scheduling delay defines the window.
            await ((== ThreadBlocked BlockedOnException) <$> threadStatus killer)
            blockConfiguration False
            result <- bounded (takeMVar completed)
            case result of
              Left exception -> fromException exception @?= Just ThreadKilled
              Right value -> assertFailure ("interrupted create returned " <> show value)
            bounded (takeMVar interrupted) >>= either throwIO pure
            submissions >>= (@?= 0)
            counts NativeNewTopic >>= (@?= (1, 1))
            assertBalanced,
      testCase "multiple configuration entries reach the broker" $
        bracket (mkProducer env) releaseProducer $ \producer -> withNativeTracking $ do
          let topic = TopicName (prefixedName env "config-multiple")
              settings = Map.fromList [("cleanup.policy", "compact"), ("retention.ms", "120000")]
          bracket
            (createTopic producer (NewTopic topic (PartitionCount 2) (ReplicationFactor 1) settings) >>= either throwIO pure)
            (\created -> deleteTopic producer created >>= (@?= Right created))
            $ \created -> do
              created @?= topic
              configValues topic "cleanup.policy" >>= (@?= ["compact"])
              configValues topic "retention.ms" >>= (@?= ["120000"])
              metadata <- topicMetadata producer (Timeout 5000) topic
              map (\t -> (tmTopicName t, map pmPartitionId (tmPartitions t))) . kmTopics <$> metadata
                @?= Right [(topic, [PartitionId 0, PartitionId 1])]
          assertBalanced,
      testCase "broker-rejected values release requests and response events repeatedly" $
        bracket (mkProducer env) releaseProducer $ \producer -> withNativeTracking $ do
          let topicsToReject = [TopicName (prefixedName env ("config-invalid-value-" <> Text.pack (show n))) | n <- [1 .. 20 :: Int]]
          forM_ topicsToReject $ \topic -> do
            result <- createTopic producer (NewTopic topic (PartitionCount 1) (ReplicationFactor 1) (Map.singleton "cleanup.policy" "not-a-policy"))
            result @?= Left (KafkaResponseError RdKafkaRespErrInvalidConfig)
            assertBalanced
          counts NativeNewTopic >>= (@?= (20, 20))
          counts NativeEvent >>= (@?= (20, 20))
          metadata <- allTopicsMetadata producer (Timeout 5000)
          (filter (`elem` topicsToReject) . map tmTopicName . kmTopics) <$> metadata @?= Right [],
      testCase "mismatched topic in an error response cannot report another topic's error" $
        bracket (mkProducer env) releaseProducer $ \producer -> withNativeTracking $ do
          let topic = TopicName (prefixedName env "config-wrong-error-topic")
          responseMode WrongTopic
          createTopic producer (NewTopic topic (PartitionCount 1) (ReplicationFactor 1) (Map.singleton "cleanup.policy" "not-a-policy"))
            >>= (@?= Left KafkaInvalidReturnValue)
          counts NativeNewTopic >>= (@?= (1, 1))
          counts NativeEvent >>= (@?= (1, 1))
          assertAbsent producer topic
          assertBalanced,
      testCase "unrepresentable partition count never wraps to broker defaults" $
        bracket (mkProducer env) releaseProducer $ \producer -> do
          let topic = TopicName (prefixedName env "config-partition-overflow")
          result <- createTopic producer (NewTopic topic (PartitionCount maxBound) (ReplicationFactor 1) mempty)
          forM_ result $ \created -> deleteTopic producer created >>= (@?= Right created)
          result @?= Left (KafkaBadSpecification "partition count is outside the C int range"),
      testCase "NUL in a topic name never creates a truncated topic" $
        bracket (mkProducer env) releaseProducer $ \producer -> do
          let prefix = TopicName (prefixedName env "config-nul-name")
              topic = TopicName (unTopicName prefix <> "\0ignored")
          flip finally (removeIfPresent producer prefix) $ withNativeTracking $ do
            result <- createTopic producer (NewTopic topic (PartitionCount 1) (ReplicationFactor 1) mempty)
            result @?= Left (KafkaBadSpecification "topic name must not contain NUL")
            counts NativeNewTopic >>= (@?= (0, 0))
            metadata <- allTopicsMetadata producer (Timeout 5000)
            (elem prefix . map tmTopicName . kmTopics) <$> metadata @?= Right False
            assertBalanced,
      testCase "unresponsive broker preserves the native timeout error" $
        withUnresponsiveBroker $ \address connected ->
          bracket
            (newProducer (brokersList [address] <> extraProps (Map.fromList [("socket.timeout.ms", "100"), ("log_level", "0")])) >>= either throwIO pure)
            releaseProducer
            $ \producer -> do
              connected
              withNativeTracking $ do
                let topic = TopicName (prefixedName env "config-unresponsive")
                result <- bounded (createTopic producer (NewTopic topic (PartitionCount 1) (ReplicationFactor 1) mempty))
                result @?= Left (KafkaResponseError RdKafkaRespErrTimedOut)
                counts NativeNewTopic >>= (@?= (1, 1))
                counts NativeEvent >>= (@?= (1, 1))
                assertBalanced
    ]
      <> boundaryTests env
      <> modeAndConcurrencyTests env
      <> timeoutDecodeTests env

-- | The default CLI runs inside the project-owned container. Other isolated
-- environments provide an rpk executable using RDKAFKA_TEST_RPK; it inherits
-- that environment's broker configuration. No shell interpolates topic names.
configValues :: TopicName -> String -> IO [String]
configValues (TopicName topic) key = do
  localRpk <- lookupEnv "RDKAFKA_TEST_RPK"
  let args = ["topic", "describe", Text.unpack topic, "-c"]
      (command, commandArgs) = case localRpk of
        Just path -> (path, args)
        Nothing -> ("container", ["exec", "rdkafka-hs-rp-0", "rpk"] <> args)
  (exit, output, errors) <- readProcessWithExitCode command commandArgs ""
  case exit of
    ExitFailure code -> assertFailure ("rpk failed (" <> show code <> "): " <> errors <> output)
    ExitSuccess -> pure [value | line <- lines output, name : value : _ <- [words line], name == key]

await :: IO Bool -> IO ()
await predicate = bounded go
  where
    go = predicate >>= \ready -> unless ready (threadDelay 1000 >> go)

bounded :: IO a -> IO a
bounded action = timeout 5000000 action >>= maybe (assertFailure "native ownership stage timed out") pure

removeIfPresent :: KafkaProducer -> TopicName -> IO ()
removeIfPresent producer topic =
  deleteTopic producer topic >>= \case
    Right actual -> actual @?= topic
    Left (KafkaResponseError RdKafkaRespErrUnknownTopicOrPart) -> pure ()
    Left err -> throwIO err

boundaryTests :: TestEnv -> [TestTree]
boundaryTests env =
  [ testCase "empty configuration creates a usable topic" $
      bracket (mkProducer env) releaseProducer $ \producer -> do
        let topic = TopicName (prefixedName env "config-empty")
        bracket
          ( withNativeTracking $ do
              result <- createTopic producer (NewTopic topic (PartitionCount 1) (ReplicationFactor 1) mempty)
              result @?= Right topic
              assertBalanced
              pure topic
          )
          (\created -> withNativeTracking $ deleteTopic producer created >>= (@?= Right created) >> assertBalanced)
          $ \created -> do
            sendMessages [ProducerRecord created (SpecifiedPartition 0) Nothing (Just "empty-configuration") mempty] producer >>= (@?= Right ())
            bracket (mkConsumerFor created (consumerProps env <> C.groupId (makeGroupId env "config-empty"))) releaseConsumer $ \consumer -> do
              records <- receiveMessages consumer 1
              map (\r -> (C.crTopic r, C.crPartition r, C.crOffset r, C.crKey r, C.crValue r, headersToList (C.crHeaders r))) <$> records
                @?= Right [(created, PartitionId 0, C.Offset 0, Nothing, Just "empty-configuration", [])],
    testCase "later setter rejection releases partially configured request" $
      bracket (mkProducer env) releaseProducer $ \producer -> withNativeTracking $ do
        let topic = TopicName (prefixedName env "config-second-rejected")
            settings = Map.fromList [("cleanup.policy", "compact"), ("retention.ms", "120000"), ("segment.ms", "60000")]
        rejectConfigurationAt 2
        createTopic producer (NewTopic topic (PartitionCount 1) (ReplicationFactor 1) settings)
          >>= (@?= Left (KafkaResponseError RdKafkaRespErrInvalidArg))
        configurationCalls >>= (@?= 2)
        submissions >>= (@?= 0)
        counts NativeNewTopic >>= (@?= (1, 1))
        assertAbsent producer topic
        assertBalanced,
    testCase "duplicate creation preserves the existing topic" $
      bracket (mkProducer env) releaseProducer $ \producer -> withNativeTracking $ do
        let topic = TopicName (prefixedName env "config-duplicate")
            specification = NewTopic topic (PartitionCount 2) (ReplicationFactor 1) mempty
        bracket (createTopic producer specification >>= either throwIO pure) (removeIfPresent producer) $ \created -> do
          created @?= topic
          createTopic producer specification >>= (@?= Left (KafkaResponseError RdKafkaRespErrTopicAlreadyExists))
          metadata <- topicMetadata producer (Timeout 5000) topic
          map (\t -> (tmTopicName t, map pmPartitionId (tmPartitions t))) . kmTopics <$> metadata
            @?= Right [(topic, [PartitionId 0, PartitionId 1])]
        assertBalanced,
    testCase "deletion removes exactly the existing topic" $
      bracket (mkProducer env) releaseProducer $ \producer -> do
        let topic = TopicName (prefixedName env "config-delete")
        bracket (createTopic producer (NewTopic topic (PartitionCount 1) (ReplicationFactor 1) mempty) >>= either throwIO pure) (removeIfPresent producer) $ \_ -> withNativeTracking $ do
          let survivor = TopicName (prefixedName env "config-delete-survivor")
          bracket
            (createTopic producer (NewTopic survivor (PartitionCount 1) (ReplicationFactor 1) mempty) >>= either throwIO pure)
            (removeIfPresent producer)
            $ \_ -> do
              deleteTopic producer topic >>= (@?= Right topic)
              awaitAbsent producer topic
              assertPresent producer survivor
          assertBalanced,
    testCase "missing topic deletion preserves its error and releases the request" $
      bracket (mkProducer env) releaseProducer $ \producer -> withNativeTracking $ do
        let topic = TopicName (prefixedName env "config-delete-missing")
        deleteTopic producer topic >>= (@?= Left (KafkaResponseError RdKafkaRespErrUnknownTopicOrPart))
        counts NativeDeleteTopic >>= (@?= (1, 1))
        assertAbsent producer topic
        assertBalanced,
    testCase "NUL deletion cannot remove a truncated victim name" $
      withTopic env "config-delete-nul-victim" 1 $ \victim producer -> withNativeTracking $ do
        deleteTopic producer (TopicName (unTopicName victim <> "\0ignored"))
          >>= (@?= Left (KafkaBadSpecification "topic name must not contain NUL"))
        counts NativeDeleteTopic >>= (@?= (0, 0))
        assertPresent producer victim
        assertBalanced,
    testCase "broker default partition and replication counts remain supported" $
      bracket (mkProducer env) releaseProducer $ \producer -> withNativeTracking $ do
        let topic = TopicName (prefixedName env "config-default-counts")
        bracket
          (createTopic producer (NewTopic topic (PartitionCount (-1)) (ReplicationFactor (-1)) mempty) >>= either throwIO pure)
          (removeIfPresent producer)
          $ \created -> do
            created @?= topic
            metadata <- topicMetadata producer (Timeout 5000) topic
            map (\t -> (tmTopicName t, map (\p -> (pmPartitionId p, length (pmReplicas p))) (tmPartitions t))) . kmTopics <$> metadata
              @?= Right [(topic, [(PartitionId 0, 1)])]
        assertBalanced,
    testCase "replication above the available broker count is rejected" $
      bracket (mkProducer env) releaseProducer $ \producer -> do
        metadata <- allTopicsMetadata producer (Timeout 5000) >>= either throwIO pure
        let topic = TopicName (prefixedName env "config-replication-unavailable")
        withNativeTracking $ do
          result <- createTopic producer (NewTopic topic (PartitionCount 1) (ReplicationFactor (length (kmBrokers metadata) + 1)) mempty)
          forM_ result (removeIfPresent producer)
          result @?= Left (KafkaResponseError RdKafkaRespErrInvalidReplicationFactor)
          assertAbsent producer topic
          assertBalanced,
    testCase "unsupported configuration follows the declared broker policy" $
      bracket (mkProducer env) releaseProducer $ \producer -> do
        suppliedPolicy <- lookupEnv "RDKAFKA_TEST_UNKNOWN_TOPIC_CONFIG_POLICY"
        policy <- case suppliedPolicy of
          Just "ignore" -> pure ("ignore" :: String)
          Just "reject" -> pure "reject"
          Nothing | teBrokers env == [BrokerAddress defaultBroker] -> pure "ignore"
          _ -> assertFailure "set RDKAFKA_TEST_UNKNOWN_TOPIC_CONFIG_POLICY to the isolated broker fixture's ignore/reject policy"
        let topic = TopicName (prefixedName env "config-unsupported")
            settings = Map.fromList [("cleanup.policy", "compact"), ("rdkafka-hs.unsupported.setting", "value")]
        flip finally (removeIfPresent producer topic) $ withNativeTracking $ do
          result <- createTopic producer (NewTopic topic (PartitionCount 1) (ReplicationFactor 1) settings)
          configurationCalls >>= (@?= 2)
          lastConfiguration >>= (@?= ("rdkafka-hs.unsupported.setting", "value"))
          if policy == "reject"
            then do
              result @?= Left (KafkaResponseError RdKafkaRespErrInvalidConfig)
              assertAbsent producer topic
            else do
              result @?= Right topic
              assertPresent producer topic
              configValues topic "cleanup.policy" >>= (@?= ["compact"])
              configValues topic "rdkafka-hs.unsupported.setting" >>= (@?= [])
          assertBalanced
  ]
    <> [ testCase name $
           bracket (mkProducer env) releaseProducer $ \producer -> withNativeTracking $ do
             let topic = TopicName (prefixedName env suffix)
             result <- createTopic producer (NewTopic topic partitions replicas settings)
             forM_ result (removeIfPresent producer)
             result @?= Left (KafkaBadSpecification reason)
             counts NativeNewTopic >>= (@?= (0, 0))
             counts NativeQueue >>= (@?= (0, 0))
             submissions >>= (@?= 0)
             assertAbsent producer topic
             assertBalanced
       | (name, suffix, partitions, replicas, settings, reason) <-
           [ ("minimum unrepresentable partition count is rejected", "config-partition-min", PartitionCount minBound, ReplicationFactor 1, mempty, "partition count is outside the C int range"),
             ("maximum unrepresentable replication count is rejected", "config-replication-max", PartitionCount 1, ReplicationFactor maxBound, mempty, "replication factor is outside the C int range"),
             ("minimum unrepresentable replication count is rejected", "config-replication-min", PartitionCount 1, ReplicationFactor minBound, mempty, "replication factor is outside the C int range"),
             ("NUL configuration key is rejected before native allocation", "config-key-nul", PartitionCount 1, ReplicationFactor 1, Map.singleton "cleanup.policy\0ignored" "compact", "topic configuration keys and values must not contain NUL"),
             ("NUL configuration value is rejected before native allocation", "config-value-nul", PartitionCount 1, ReplicationFactor 1, Map.singleton "cleanup.policy" "compact\0ignored", "topic configuration keys and values must not contain NUL")
           ]
       ]
    <> [ testCase name $
           bracket (mkProducer env) releaseProducer $ \producer -> withNativeTracking $ do
             let topic = TopicName (prefixedName env suffix)
             result <- createTopic producer (NewTopic topic partitions replicas mempty)
             case result of
               Left (KafkaError message) -> assertBool (show message) (reason `Text.isPrefixOf` message)
               other -> do
                 forM_ other (removeIfPresent producer)
                 assertFailure ("expected native constructor validation, got " <> show other)
             counts NativeNewTopic >>= (@?= (0, 0))
             submissions >>= (@?= 0)
             assertAbsent producer topic
             assertBalanced
       | (name, suffix, partitions, replicas, reason) <-
           [ ("negative invalid partition count retains native validation", "config-partition-negative", PartitionCount (-2), ReplicationFactor 1, "num_partitions out of expected range"),
             ("maximum C partition count retains native validation", "config-partition-cmax", PartitionCount (fromIntegral (maxBound :: CInt)), ReplicationFactor 1, "num_partitions out of expected range"),
             ("negative invalid replication count retains native validation", "config-replication-negative", PartitionCount 1, ReplicationFactor (-2), "replication_factor out of expected range"),
             ("maximum C replication count retains native validation", "config-replication-cmax", PartitionCount 1, ReplicationFactor (fromIntegral (maxBound :: CInt)), "replication_factor out of expected range")
           ]
       ]
    <> [ testCase name $
           bracket (mkProducer env) releaseProducer $ \producer -> withNativeTracking $ do
             let topic = if suffix == "" then TopicName "" else TopicName (prefixedName env suffix)
             result <- createTopic producer (NewTopic topic partitions replicas mempty)
             forM_ result (removeIfPresent producer)
             result @?= Left (KafkaResponseError expected)
             assertAbsent producer topic
             assertBalanced
       | (name, suffix, partitions, replicas, expected) <-
           [ ("empty topic name is rejected by the broker", "", PartitionCount 1, ReplicationFactor 1, RdKafkaRespErrTopicException),
             ("space in a topic name is rejected by the broker", "config invalid name", PartitionCount 1, ReplicationFactor 1, RdKafkaRespErrTopicException),
             ("overlong topic name is rejected by the broker", "config-long-" <> Text.replicate 250 "x", PartitionCount 1, ReplicationFactor 1, RdKafkaRespErrTopicException),
             ("zero partition count is rejected by the broker", "config-partition-zero", PartitionCount 0, ReplicationFactor 1, RdKafkaRespErrInvalidPartitions),
             ("zero replication count is rejected by the broker", "config-replication-zero", PartitionCount 1, ReplicationFactor 0, RdKafkaRespErrInvalidReplicationFactor)
           ]
       ]
    <> [ testCase name $
           bracket (shortRequestProducer env) releaseProducer $ \producer -> do
             let topic = TopicName (prefixedName env suffix)
             flip finally (removeIfPresent producer topic) $ withNativeTracking $ do
               responseMode mode
               result <- createTopic producer (NewTopic topic (PartitionCount 1) (ReplicationFactor 1) mempty)
               result @?= Left expected
               counts NativeEvent >>= (@?= (1, 1))
               -- The broker completed creation before the shim damaged/dropped
               -- its response. Observe that state and always remove the topic.
               assertPresent producer topic
               assertBalanced
       | (name, suffix, mode, expected) <-
           [ ("wrong event type fails and releases its event", "config-wrong-event", WrongEvent, KafkaInvalidReturnValue),
             ("empty response fails and releases its event", "config-empty-result", EmptyResult, KafkaInvalidReturnValue),
             ("wrong topic response cannot report success", "config-wrong-result", WrongTopic, KafkaInvalidReturnValue),
             ("missing polled event reports a timeout", "config-missing-event", MissingPoll, KafkaResponseError RdKafkaRespErrTimedOut)
           ]
       ]
    <> [ testCase "delayed response succeeds with balanced ownership" $
           bracket (mkProducer env) releaseProducer $ \producer -> withNativeTracking $ do
             let topic = TopicName (prefixedName env "config-delayed")
             responseMode DelayedResponse
             start <- getMonotonicTime
             bracket
               (bounded (createTopic producer (NewTopic topic (PartitionCount 1) (ReplicationFactor 1) mempty)) >>= either throwIO pure)
               (removeIfPresent producer)
               $ \created -> do
                 created @?= topic
                 elapsed <- subtract start <$> getMonotonicTime
                 assertBool ("response delay was not exercised: " <> show elapsed) (elapsed >= 0.1)
                 assertPresent producer topic
             assertBalanced
       ]

assertAbsent :: KafkaProducer -> TopicName -> IO ()
assertAbsent producer topic = do
  metadata <- allTopicsMetadata producer (Timeout 5000)
  (elem topic . map tmTopicName . kmTopics) <$> metadata @?= Right False

assertPresent :: KafkaProducer -> TopicName -> IO ()
assertPresent producer topic = do
  metadata <- allTopicsMetadata producer (Timeout 5000)
  (elem topic . map tmTopicName . kmTopics) <$> metadata @?= Right True

awaitAbsent :: KafkaProducer -> TopicName -> IO ()
awaitAbsent producer topic = bounded go
  where
    go = do
      metadata <- allTopicsMetadata producer (Timeout 5000) >>= either throwIO pure
      if topic `elem` map tmTopicName (kmTopics metadata)
        then threadDelay 10000 >> go
        else pure ()

modeAndConcurrencyTests :: TestEnv -> [TestTree]
modeAndConcurrencyTests env =
  [ testCase ("consumer administration applies settings in " <> label <> " mode")
      $ bracket
        (C.newAssignedConsumer (consumerProps env <> C.groupId (makeGroupId env ("config-consumer-" <> Text.pack label)) <> C.callbackPollMode mode) >>= either throwIO pure)
        releaseConsumer
      $ \consumer -> do
        when closeFirst (C.closeConsumer consumer >>= (@?= Nothing))
        withNativeTracking $ do
          let topic = TopicName (prefixedName env ("config-consumer-" <> Text.pack label))
          bracket
            (createTopic consumer (NewTopic topic (PartitionCount 1) (ReplicationFactor 1) (Map.singleton "cleanup.policy" "compact")) >>= either throwIO pure)
            (\created -> deleteTopic consumer created >>= (@?= Right created))
            $ \created -> do
              created @?= topic
              configValues topic "cleanup.policy" >>= (@?= ["compact"])
              metadata <- allTopicsMetadata consumer (Timeout 5000)
              (elem topic . map tmTopicName . kmTopics) <$> metadata @?= Right True
          counts NativeNewTopic >>= (@?= (1, 1))
          counts NativeDeleteTopic >>= (@?= (1, 1))
          assertBalanced
  | (mode, label, closeFirst) <- [(C.CallbackPollModeAsync, "async", False), (C.CallbackPollModeSync, "sync", False), (C.CallbackPollModeAsync, "async-closed", True), (C.CallbackPollModeSync, "sync-closed", True)]
  ]
    <> [ testCase "concurrent requests keep their own queues and responses" $
           bracket (newProducer (producerProps env <> extraProps (Map.singleton "socket.timeout.ms" "10000")) >>= either throwIO pure) releaseProducer $ \producer -> do
             let topics = [TopicName (prefixedName env ("config-concurrent-" <> Text.pack (show n))) | n <- [1 .. 20 :: Int]]
                 spawn topic = do
                   done <- newEmptyMVar
                   thread <- forkFinally (createTopic producer (NewTopic topic (PartitionCount 1) (ReplicationFactor 1) (Map.singleton "cleanup.policy" "compact"))) (putMVar done)
                   pure (topic, thread, done)
                 joinResult done = timeout 12000000 (readMVar done) >>= maybe (assertFailure "concurrent admin request exceeded its native deadline") pure
                 join workers = do
                   forM_ workers (\(_, thread, _) -> killThread thread)
                   forM_ workers (\(_, _, done) -> joinResult done)
             flip finally (mapM_ (removeIfPresent producer) topics) $ withNativeTracking $ do
               bracket (mapM spawn topics) join $ \workers ->
                 forM_ workers $ \(topic, _, done) -> do
                   result <- joinResult done
                   either throwIO (\value -> value @?= Right topic) result
               counts NativeNewTopic >>= (@?= (20, 20))
               peak NativeQueue >>= \high -> assertBool ("no native queue overlap observed: " <> show high) (high > 1)
               metadata <- allTopicsMetadata producer (Timeout 5000)
               (sort . map (\t -> (tmTopicName t, map pmPartitionId (tmPartitions t))) . filter ((`elem` topics) . tmTopicName) . kmTopics) <$> metadata
                 @?= Right (sort [(topic, [PartitionId 0]) | topic <- topics])
               assertBalanced,
         testCase "unresponsive deletion preserves the native timeout error" $
           withUnresponsiveBroker $ \address connected ->
             bracket
               (newProducer (brokersList [address] <> extraProps (Map.fromList [("socket.timeout.ms", "100"), ("log_level", "0")])) >>= either throwIO pure)
               releaseProducer
               $ \producer -> do
                 connected
                 withNativeTracking $ do
                   let topic = TopicName (prefixedName env "config-delete-unresponsive")
                   bounded (deleteTopic producer topic) >>= (@?= Left (KafkaResponseError RdKafkaRespErrTimedOut))
                   counts NativeDeleteTopic >>= (@?= (1, 1))
                   counts NativeEvent >>= (@?= (1, 1))
                   assertBalanced
       ]
    <> [ testCase name $
           bracket (shortRequestProducer env) releaseProducer $ \producer -> do
             let topic = TopicName (prefixedName env suffix)
             bracket
               (createTopic producer (NewTopic topic (PartitionCount 1) (ReplicationFactor 1) mempty) >>= either throwIO pure)
               (removeIfPresent producer)
               $ \created -> withNativeTracking $ do
                 responseMode mode
                 deleteTopic producer created >>= (@?= Left expected)
                 counts NativeDeleteTopic >>= (@?= (1, 1))
                 counts NativeEvent >>= (@?= (1, 1))
                 awaitAbsent producer topic
                 assertBalanced
       | (name, suffix, mode, expected) <-
           [ ("deletion rejects a wrong event type and releases it", "config-delete-wrong-event", WrongEvent, KafkaInvalidReturnValue),
             ("deletion rejects an empty result and releases its event", "config-delete-empty-result", EmptyResult, KafkaInvalidReturnValue),
             ("deletion rejects a mismatched topic result", "config-delete-wrong-topic", WrongTopic, KafkaInvalidReturnValue),
             ("deletion reports a missing response as a timeout", "config-delete-missing-event", MissingPoll, KafkaResponseError RdKafkaRespErrTimedOut)
           ]
       ]

shortRequestProducer :: TestEnv -> IO KafkaProducer
shortRequestProducer env =
  newProducer (producerProps env <> extraProps (Map.singleton "socket.timeout.ms" "1000")) >>= either throwIO pure

-- Native configuration is immutable and validated, but malformed/error replies
-- from its getter still return a defined error before acquiring admin handles.
timeoutDecodeTests :: TestEnv -> [TestTree]
timeoutDecodeTests env =
  [ testCase name $
      bracket (mkProducer env) releaseProducer $ \producer -> withNativeTracking $ do
        let topic = TopicName (prefixedName env ("config-timeout-decode-" <> Text.pack (show mode)))
        timeoutFault mode
        createTopic producer (NewTopic topic (PartitionCount 1) (ReplicationFactor 1) mempty)
          >>= (@?= Left (KafkaError reason))
        timeoutReads >>= (@?= 1)
        counts NativeNewTopic >>= (@?= (0, 0))
        counts NativeQueue >>= (@?= (0, 0))
        submissions >>= (@?= 0)
        assertAbsent producer topic
        assertBalanced
  | (name, mode, reason) <-
      [ ("missing native timeout configuration returns an error", 1, "could not read socket.timeout.ms from the native client"),
        ("malformed native timeout configuration returns an error", 2, "invalid socket.timeout.ms value from the native client"),
        ("zero native timeout configuration returns an error", 3, "invalid socket.timeout.ms value from the native client"),
        ("negative native timeout configuration returns an error", 4, "invalid socket.timeout.ms value from the native client"),
        ("overflowing native timeout configuration returns an error", 5, "invalid socket.timeout.ms value from the native client"),
        ("oversized native timeout configuration never reads a truncated buffer", 6, "could not read socket.timeout.ms from the native client")
      ]
  ]
