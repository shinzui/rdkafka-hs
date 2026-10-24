-- | The integration suite's environment: which broker to use, the per-run name
-- prefix that isolates one run from another, client properties, and tasty
-- fixtures that create and release clients.
--
-- The suite only ever talks to the project's private Redpanda broker. The
-- broker comes from @RDKAFKA_TEST_BROKER@ (default 'defaultBroker'), and
-- 'resolveBrokers' rejects the machine-global broker on loopback port 9092
-- before any client is created, because the suite creates and deletes topics.
module Kafka.TestEnv
  ( -- * Environment
    TestEnv (..),
    defaultBroker,
    resolveBrokers,
    newTestEnv,
    checkBrokerReachable,

    -- * Prefixed names
    prefixedName,
    testGroupId,
    makeGroupId,
    isTestGroupId,
    isTestTopic,

    -- * Client properties
    consumerProps,
    consumerPropsNoStore,
    producerProps,
    testSubscription,

    -- * Fixtures
    mkProducer,
    mkConsumerWith,
    mkConsumerFor,
    withProducer,
    withConsumer,
    withKafka,
    withTopic,
    produceTo,

    -- * Messages
    receiveMessages,
    receiveTestMessages,
    releaseConsumer,
    releaseProducer,
    testMessages,
    sendMessages,
    sendMessagesWithHeaders,
  )
where

import Control.Concurrent (threadDelay)
import Control.Concurrent.MVar (newEmptyMVar, tryPutMVar, tryReadMVar)
import Control.Exception (bracket, bracketOnError, finally, throwIO)
import Control.Monad (void)
import Data.ByteString qualified as BS
import Data.Char (isDigit)
import Data.Text (Text)
import Data.Text qualified as Text
import GHC.Clock (getMonotonicTimeNSec)
import Kafka.Consumer as C
import Kafka.FixtureSupport (acquirePair, collectExactly, sendAndConfirm)
import Kafka.Metadata (allTopicsMetadata)
import Kafka.Producer as P
import Kafka.Topic (NewTopic (..), PartitionCount (..), ReplicationFactor (..), createTopic, deleteTopic)
import System.Random qualified as Rnd
import System.Timeout (timeout)
import Test.Tasty (TestTree, withResource)

-- | Everything a test group needs to know about this run.
data TestEnv = TestEnv
  { -- | the private broker's bootstrap addresses
    teBrokers :: [BrokerAddress],
    -- | a random prefix carried by every topic and consumer group of this run
    tePrefix :: Text,
    -- | the run's main topic, @<prefix>-topic@
    teTopic :: TopicName
  }

-- | The private broker's Kafka listener: @redpanda.kafka-port@ in
-- @.seihou/manifest.json@.
defaultBroker :: Text
defaultBroker = "127.0.0.1:39092"

-- | Turn the value of @RDKAFKA_TEST_BROKER@ (a comma-separated list of
-- @host:port@ addresses, or nothing for 'defaultBroker') into broker
-- addresses, refusing the machine-global broker.
--
-- An address on a loopback host whose port is 9092, or that has no port (which
-- librdkafka treats as 9092), is rejected.
resolveBrokers :: Maybe String -> Either String [BrokerAddress]
resolveBrokers raw = do
  let value = maybe defaultBroker (Text.strip . Text.pack) raw
      addrs = filter (not . Text.null) (Text.strip <$> Text.splitOn "," value)
  if null addrs
    then Left "RDKAFKA_TEST_BROKER is empty; unset it to use the private broker at 127.0.0.1:39092."
    else traverse checkAddress addrs
  where
    checkAddress addr
      | isSharedBroker addr =
          Left $
            "RDKAFKA_TEST_BROKER names "
              <> Text.unpack addr
              <> ", the machine-global broker. The integration suite creates and deletes topics,"
              <> " so it only runs against the project's private broker; start it with"
              <> " `just redpanda-up` and unset RDKAFKA_TEST_BROKER (default 127.0.0.1:39092)."
      | otherwise = Right (BrokerAddress addr)

isSharedBroker :: Text -> Bool
isSharedBroker addr =
  let (host, port) = splitHostPort addr
   in isLoopback (Text.toLower host) && maybe True (== 9092) port

-- | Split @host:port@, @[v6]:port@, @host@ or @[v6]@. A bare IPv6 literal
-- without brackets has no port.
splitHostPort :: Text -> (Text, Maybe Int)
splitHostPort addr
  | Just rest <- Text.stripPrefix "[" addr =
      let (host, after) = Text.breakOn "]" rest
       in (host, Text.stripPrefix "]:" after >>= readPort)
  | Text.count ":" addr == 1 =
      let (host, port) = Text.breakOn ":" addr
       in (host, readPort (Text.drop 1 port))
  | otherwise = (addr, Nothing)
  where
    readPort p
      | not (Text.null p) && Text.all isDigit p = Just (read (Text.unpack p))
      | otherwise = Nothing

isLoopback :: Text -> Bool
isLoopback host =
  host `elem` ["localhost", "localhost.localdomain", "::1", "0:0:0:0:0:0:0:1", "0.0.0.0", "::"]
    || "127." `Text.isPrefixOf` host

-- | An environment for one run, with a fresh random ten-letter prefix.
newTestEnv :: [BrokerAddress] -> IO TestEnv
newTestEnv brokers = do
  prefix <- Text.pack . take 10 . Rnd.randomRs ('a', 'z') <$> Rnd.newStdGen
  pure
    TestEnv
      { teBrokers = brokers,
        tePrefix = prefix,
        teTopic = TopicName (prefix <> "-topic")
      }

-- | Ask the broker for metadata, giving up after ten seconds.
checkBrokerReachable :: TestEnv -> IO (Either String ())
checkBrokerReachable env = do
  res <- newProducer (P.brokersList (teBrokers env))
  case res of
    Left err -> pure (Left ("could not create a probe client: " <> show err))
    Right prod -> do
      meta <- allTopicsMetadata prod (Timeout 10000) `finally` releaseProducer prod
      pure $ case meta of
        Right _ -> Right ()
        Left err ->
          Left $
            "the Kafka broker at "
              <> Text.unpack (Text.intercalate "," (unBrokerAddress <$> teBrokers env))
              <> " did not answer a metadata request within 10 seconds ("
              <> show err
              <> "). Start the private broker with `just redpanda-up`."

-- | @<prefix>-<suffix>@.
prefixedName :: TestEnv -> Text -> Text
prefixedName env suffix = tePrefix env <> "-" <> suffix

-- | The run's shared consumer group, named after the prefix alone.
testGroupId :: TestEnv -> ConsumerGroupId
testGroupId env = ConsumerGroupId (tePrefix env)

makeGroupId :: TestEnv -> Text -> ConsumerGroupId
makeGroupId env = ConsumerGroupId . prefixedName env

isTestGroupId :: TestEnv -> ConsumerGroupId -> Bool
isTestGroupId env (ConsumerGroupId group) = tePrefix env `Text.isPrefixOf` group

isTestTopic :: TestEnv -> TopicName -> Bool
isTestTopic env (TopicName name) = (tePrefix env <> "-") `Text.isPrefixOf` name

consumerProps :: TestEnv -> ConsumerProperties
consumerProps env =
  C.brokersList (teBrokers env)
    <> groupId (testGroupId env)
    <> C.setCallback (logCallback (\l s1 s2 -> print $ "[Consumer] " <> show l <> ": " <> s1 <> ", " <> s2))
    <> C.setCallback (errorCallback (\e r -> print $ "[Consumer] " <> show e <> ": " <> r))
    <> noAutoCommit

consumerPropsNoStore :: TestEnv -> ConsumerProperties
consumerPropsNoStore env = consumerProps env <> noAutoOffsetStore

producerProps :: TestEnv -> ProducerProperties
producerProps env =
  P.brokersList (teBrokers env)
    <> P.setCallback (logCallback (\l s1 s2 -> print $ "[Producer] " <> show l <> ": " <> s1 <> ", " <> s2))
    <> P.setCallback (errorCallback (\e r -> print $ "[Producer] " <> show e <> ": " <> r))

testSubscription :: TopicName -> Subscription
testSubscription t =
  topics [t]
    <> offsetReset Earliest

mkProducer :: TestEnv -> IO KafkaProducer
mkProducer env =
  newProducer (producerProps env) >>= either (throwIO . userError . ("newProducer: " <>) . show) pure

-- | Subscribe a consumer to the run's topic and return once its first
-- partition assignment has been delivered, or fail after thirty seconds.
--
-- In 'CallbackPollModeSync' librdkafka only serves the rebalance callback from
-- inside a poll, so this keeps polling for events (without blocking, so no
-- record can be fetched and discarded) while it waits. In
-- 'CallbackPollModeAsync' the consumer's background loop serves it, and
-- polling here could discard a record that races the partition-queue redirect.
mkConsumerWith :: TestEnv -> ConsumerProperties -> IO KafkaConsumer
mkConsumerWith env = mkConsumerFor (teTopic env)

-- | Like 'mkConsumerWith', subscribed to the given topic.
mkConsumerFor :: TopicName -> ConsumerProperties -> IO KafkaConsumer
mkConsumerFor topic props = do
  assigned <- newEmptyMVar
  let props' = C.setCallback (rebalanceCallback (\_ evt -> onRebalance assigned evt)) <> props
      driveCallbacks c = case effectiveCallbackPollMode props' of
        CallbackPollModeSync -> pollConsumerEvents c Nothing
        CallbackPollModeAsync -> pure ()
  let acquire =
        newConsumer props' (testSubscription topic)
          >>= either (throwIO . userError . ("newConsumer: " <>) . show) pure
  bracketOnError acquire destroyConsumer $ \consumer -> do
    let awaitAssignment =
          tryReadMVar assigned >>= \case
            Just () -> pure ()
            Nothing -> do
              driveCallbacks consumer
              threadDelay 50000
              awaitAssignment
    timeout 30000000 awaitAssignment >>= \case
      Just () -> pure consumer
      Nothing -> throwIO (userError "consumer was not assigned a partition within 30 seconds")
  where
    onRebalance var = \case
      RebalanceAssign _ -> void (tryPutMVar var ())
      _ -> pure ()

withProducer :: TestEnv -> (IO KafkaProducer -> TestTree) -> TestTree
withProducer env = withResource (mkProducer env) releaseProducer

withConsumer :: TestEnv -> ConsumerProperties -> (IO KafkaConsumer -> TestTree) -> TestTree
withConsumer env props = withResource (mkConsumerWith env props) releaseConsumer

withKafka :: TestEnv -> ConsumerProperties -> (IO (KafkaConsumer, KafkaProducer) -> TestTree) -> TestTree
withKafka env props =
  withResource
    (acquirePair (mkConsumerWith env props) releaseConsumer (mkProducer env))
    (\(consumer, producer) -> releaseProducer producer `finally` releaseConsumer consumer)

-- | Create a topic named @<prefix>-<suffix>@ with the given number of
-- partitions, run the action with it and a producer, then delete the topic.
withTopic :: TestEnv -> Text -> Int -> (TopicName -> KafkaProducer -> IO a) -> IO a
withTopic env suffix partitions action =
  bracket (mkProducer env) releaseProducer $ \producer -> do
    let topic = TopicName (prefixedName env suffix)
        create =
          createTopic producer (NewTopic topic (PartitionCount partitions) (ReplicationFactor 1) mempty)
            >>= either (throwIO . userError . ("createTopic: " <>) . show) (checkTopic topic)
        remove =
          deleteTopic producer topic
            >>= either (throwIO . userError . ("deleteTopic: " <>) . show) (checkTopic topic)
    bracket create (const remove) (const (action topic producer))
  where
    checkTopic expected actual
      | expected == actual = pure ()
      | otherwise = throwIO (userError ("admin response topic mismatch: " <> show (expected, actual)))

releaseConsumer :: KafkaConsumer -> IO ()
releaseConsumer consumer =
  (closeConsumer consumer >>= mapM_ throwIO) `finally` destroyConsumer consumer

-- | Cleanup must not wait forever for an unreachable broker. Purge first;
-- this fixture is cleanup, delivery success is checked by sendMessages.
releaseProducer :: KafkaProducer -> IO ()
releaseProducer producer = do
  purgeProducer producer [PurgeQueue, PurgeInFlight] >>= mapM_ throwIO
  flushProducerTimeout producer (Timeout 5000) >>= mapM_ throwIO

-- | Produce one record and verify the matching successful delivery report.
produceTo :: KafkaProducer -> TopicName -> Int -> BS.ByteString -> IO ()
produceTo producer topic partition value =
  sendMessages [ProducerRecord topic (SpecifiedPartition partition) Nothing (Just value) mempty] producer
    >>= either throwIO pure

receiveMessages :: KafkaConsumer -> Int -> IO (Either KafkaError [ConsumerRecord (Maybe BS.ByteString) (Maybe BS.ByteString)])
receiveMessages kafka count = collectExactly getMonotonicTimeNSec 30000000000 count (pollMessage kafka (Timeout 200))

-- | Validate the inherited scenario's complete payload/key/header identities
-- and contiguous offsets, not just its count. Every send uses testMessages.
receiveTestMessages :: TestEnv -> KafkaConsumer -> Int -> Int -> IO (Either KafkaError [ConsumerRecord (Maybe BS.ByteString) (Maybe BS.ByteString)])
receiveTestMessages env consumer count start = do
  result <- receiveMessages consumer count
  pure $ do
    records <- result
    let expected = take count (cycle (testMessages (teTopic env)))
        actual = [ProducerRecord (crTopic r) UnassignedPartition (crKey r) (crValue r) (crHeaders r) | r <- records]
    if actual == expected
      && map crPartition records == replicate count (PartitionId 0)
      && map crOffset records == map (Offset . fromIntegral) [start .. start + count - 1]
      then Right records
      else Left (KafkaError ("unexpected record identities/offsets: " <> Text.pack (show records)))

testMessages :: TopicName -> [ProducerRecord]
testMessages t =
  [ ProducerRecord t UnassignedPartition Nothing (Just "test from producer") mempty,
    ProducerRecord t UnassignedPartition (Just "key") (Just "test from producer (with key)") mempty
  ]

sendMessages :: [ProducerRecord] -> KafkaProducer -> IO (Either KafkaError ())
sendMessages msgs prod = sendAndConfirm 10000000 (produceMessage' prod) (flushProducerTimeout prod) msgs

sendMessagesWithHeaders :: [ProducerRecord] -> Headers -> KafkaProducer -> IO (Either KafkaError ())
sendMessagesWithHeaders msgs hdrs = sendMessages [msg {prHeaders = hdrs} | msg <- msgs]
