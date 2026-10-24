module Kafka.PublicDataContractSpec (tests) where

import Control.Exception (displayException, fromException, toException)
import Control.Monad (forM_)
import Data.ByteString qualified as BS
import Data.Int (Int64)
import Data.List (intercalate)
import Data.List.NonEmpty (NonEmpty (..))
import Data.Map.Strict qualified as M
import Data.Semigroup (sconcat, stimes)
import Data.Set qualified as Set
import Data.String (fromString)
import Data.Text qualified as Text
import Foreign.C.String (CString, peekCString)
import Foreign.C.Types (CInt (..))
import GHC.Generics qualified as G
import Kafka.PropertySupport
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertBool, testCase, (@?=))
import Test.Tasty.QuickCheck qualified as QC
import "rdkafka-hs" Kafka.Consumer qualified as C
import "rdkafka-hs" Kafka.Metadata qualified as D
import "rdkafka-hs" Kafka.Producer qualified as P
import "rdkafka-hs" Kafka.Topic.Types qualified as T
import "rdkafka-hs" Kafka.Types qualified as K

foreign import ccall unsafe "rdkafka_hs_test_error_count" errorCount :: IO CInt

foreign import ccall unsafe "rdkafka_hs_test_error_code" errorCode :: CInt -> IO CInt

foreign import ccall unsafe "rdkafka_hs_test_error_name" errorName :: CInt -> IO CString

foreign import ccall unsafe "rdkafka_hs_test_error_description" errorDescription :: CInt -> IO CString

foreign import ccall unsafe "rdkafka_hs_test_error_sentinel" sentinel :: CInt -> IO CInt

tests :: TestTree
tests =
  testGroup
    "Public data contracts"
    [ generatedGroup
        "builders"
        [ QC.testProperty "semigroup defaults retain writes callbacks and subscription union" $
            QC.forAll ((,,) <$> text <*> text <*> QC.choose (1, 5 :: Int)) $ \(key, value, copies) ->
              let cp = C.extraProp key value <> C.setCallback (C.logCallback (\_ _ _ -> pure ()))
                  pp = P.extraProp key value <> P.setCallback (P.deliveryCallback (\_ -> pure ()))
                  sub = C.topics [K.TopicName key] <> C.extraSubscriptionProps (M.singleton key value)
                  subView (C.Subscription names properties) = (Set.toAscList names, properties)
               in QC.conjoin
                    [ consumer (sconcat (cp :| [cp])) QC.=== (M.singleton key value, Nothing, Nothing, 2),
                      consumer (stimes copies cp) QC.=== (M.singleton key value, Nothing, Nothing, copies),
                      producer (sconcat (pp :| [pp])) QC.=== (M.singleton key value, M.empty, Nothing, 2),
                      producer (stimes copies pp) QC.=== (M.singleton key value, M.empty, Nothing, copies),
                      subView (sconcat (sub :| [sub])) QC.=== ([K.TopicName key], M.singleton key value),
                      subView (stimes copies sub) QC.=== ([K.TopicName key], M.singleton key value)
                    ],
          QC.testProperty "consumer builders preserve exact settings and leave other fields unset" $
            QC.forAll ((,,,,) <$> strings <*> text <*> text <*> signed <*> signed64) $ \(addresses, group, client, amount, millis) ->
              let property pairs = (M.fromList pairs, Nothing, Nothing, 0)
                  decimal value = Text.pack (show value)
               in QC.conjoin
                    [ consumer (C.brokersList (map K.BrokerAddress addresses)) QC.=== property [("bootstrap.servers", Text.pack (intercalate "," (map Text.unpack addresses)))],
                      consumer (C.groupId (C.ConsumerGroupId group)) QC.=== property [("group.id", group)],
                      consumer (C.clientId (K.ClientId client)) QC.=== property [("client.id", client)],
                      consumer (C.autoCommit (K.Millis millis)) QC.=== property [("enable.auto.commit", "true"), ("auto.commit.interval.ms", decimal millis)],
                      consumer C.noAutoCommit QC.=== property [("enable.auto.commit", "false")],
                      consumer C.noAutoOffsetStore QC.=== property [("enable.auto.offset.store", "false")],
                      consumer C.suppressDisconnectLogs QC.=== property [("log.connection.close", "false")],
                      consumer (C.statisticsInterval (K.Millis millis)) QC.=== property [("statistics.interval.ms", decimal millis)],
                      consumer (C.queuedMaxMessagesKBytes amount) QC.=== property [("queued.max.messages.kbytes", decimal amount)],
                      consumer (C.extraProps (M.fromList [(group, client), (client, group)])) QC.=== property [(group, client), (client, group)]
                    ],
          QC.testProperty "producer builders preserve global versus explicit topic placement" $
            QC.forAll ((,,) <$> strings <*> signed <*> signed64) $ \(addresses, amount, millis) ->
              let global pairs = (M.fromList pairs, M.empty, Nothing, 0)
                  topic pairs = (M.empty, M.fromList pairs, Nothing, 0)
               in QC.conjoin
                    [ producer (P.brokersList (map K.BrokerAddress addresses)) QC.=== global [("bootstrap.servers", Text.pack (intercalate "," (map Text.unpack addresses)))],
                      producer P.suppressDisconnectLogs QC.=== global [("log.connection.close", "false")],
                      producer (P.statisticsInterval (K.Millis millis)) QC.=== global [("statistics.interval.ms", Text.pack (show millis))],
                      producer (P.sendTimeout (K.Timeout amount)) QC.=== topic [("message.timeout.ms", Text.pack (show amount))]
                    ],
          QC.testProperty "all codecs have independently specified builder spellings" $
            QC.forAll (QC.elements codecs) $ \(codec, spelling) ->
              QC.conjoin
                [ consumer (C.compression codec) QC.=== (M.singleton "compression.codec" spelling, Nothing, Nothing, 0),
                  producer (P.compression codec) QC.=== (M.singleton "compression.codec" spelling, M.empty, Nothing, 0),
                  producer (P.topicCompression codec) QC.=== (M.empty, M.singleton "compression.codec" spelling, Nothing, 0)
                ],
          QC.testProperty "debug contexts preserve order duplicates and the empty-list identity" $
            QC.forAll (boundedList 15 (QC.elements contexts)) $ \entries ->
              let settings = if null entries then M.empty else M.singleton "debug" (Text.pack (intercalate "," (map snd entries)))
               in QC.conjoin [consumer (C.debugOptions (map fst entries)) QC.=== (settings, Nothing, Nothing, 0), producer (P.debugOptions (map fst entries)) QC.=== (settings, M.empty, Nothing, 0)]
        ],
      generatedGroup
        "representations"
        [ QC.testProperty "numeric ordering and reads agree with represented signed values" $
            QC.forAll ((,,,) <$> signed <*> signed <*> signed64 <*> signed64) $ \(a, b, x, y) ->
              QC.conjoin
                [ compare (K.BrokerId a) (K.BrokerId b) QC.=== compare a b,
                  compare (K.PartitionId a) (K.PartitionId b) QC.=== compare a b,
                  compare (K.BatchSize a) (K.BatchSize b) QC.=== compare a b,
                  compare (C.Offset x) (C.Offset y) QC.=== compare x y,
                  compare (K.Millis x) (K.Millis y) QC.=== compare x y,
                  readShow (K.BrokerId a),
                  readShow (K.PartitionId a),
                  readShow (K.BatchSize a),
                  readShow (K.Timeout a),
                  readShow (C.Offset x),
                  readShow (K.Millis x),
                  fromEnum (K.PartitionId a) QC.=== a,
                  (toEnum a :: K.PartitionId) QC.=== K.PartitionId a
                ],
          QC.testProperty "numeric instances preserve primitive arithmetic including boundaries" $
            QC.forAll ((,,,) <$> signed <*> signed <*> signed64 <*> signed64) $ \(a, b, x, y) ->
              QC.conjoin
                [ map K.unBatchSize [K.BatchSize a + K.BatchSize b, K.BatchSize a - K.BatchSize b, K.BatchSize a * K.BatchSize b, negate (K.BatchSize a), abs (K.BatchSize a), signum (K.BatchSize a), fromInteger (toInteger a)] QC.=== [a + b, a - b, a * b, negate a, abs a, signum a, a],
                  map K.unMillis [K.Millis x + K.Millis y, K.Millis x - K.Millis y, K.Millis x * K.Millis y, negate (K.Millis x), abs (K.Millis x), signum (K.Millis x), fromInteger (toInteger x)] QC.=== [x + y, x - y, x * y, negate x, abs x, signum x, x]
                ],
          QC.testProperty "text ordering string conversion reads and generic representations round-trip" $
            QC.forAll ((,) <$> text <*> text) $ \(a, b) ->
              QC.conjoin
                [ compare (K.TopicName a) (K.TopicName b) QC.=== compare a b,
                  compare (K.ClientId a) (K.ClientId b) QC.=== compare a b,
                  compare (C.ConsumerGroupId a) (C.ConsumerGroupId b) QC.=== compare a b,
                  compare (D.GroupMemberId a) (D.GroupMemberId b) QC.=== compare a b,
                  compare (D.GroupProtocol a) (D.GroupProtocol b) QC.=== compare a b,
                  compare (D.GroupProtocolType a) (D.GroupProtocolType b) QC.=== compare a b,
                  (fromString (Text.unpack a) :: K.TopicName) QC.=== K.TopicName a,
                  (fromString (Text.unpack a) :: K.ClientId) QC.=== K.ClientId a,
                  (fromString (Text.unpack a) :: K.BrokerAddress) QC.=== K.BrokerAddress a,
                  (fromString (Text.unpack a) :: C.ConsumerGroupId) QC.=== C.ConsumerGroupId a,
                  readShow (K.TopicName a),
                  readShow (D.GroupMemberId a),
                  readShow (D.GroupProtocol a),
                  readShow (D.GroupProtocolType a),
                  generic (K.TopicName a),
                  generic (K.ClientId a),
                  generic (K.BrokerAddress a),
                  generic (C.ConsumerGroupId a)
                ],
          QC.testProperty "consumer variants retain fields and distinguish constructors" $
            QC.forAll ((,,,) <$> text <*> signed <*> signed64 <*> bytes) $ \(name, partition, offset, value) ->
              let topic = K.TopicName name
                  partitions = [(topic, K.PartitionId partition)]
                  offsets = [C.PartitionOffsetBeginning, C.PartitionOffsetEnd, C.PartitionOffset offset, C.PartitionOffsetStored, C.PartitionOffsetInvalid]
                  events = [C.RebalanceBeforeAssign partitions, C.RebalanceAssign partitions, C.RebalanceBeforeRevoke partitions, C.RebalanceRevoke partitions]
                  headers = K.headersFromList [(value, value), (value, "")]
                  message = C.ConsumerRecord topic (K.PartitionId partition) (C.Offset offset) (C.CreateTime (K.Millis offset)) headers value value
                  failure = C.ConsumerMessageError K.KafkaInvalidReturnValue (Just topic) (Just (K.PartitionId partition)) (Just (C.Offset offset))
                  stores = [C.OffsetStoreBroker, C.OffsetStoreFile (Text.unpack name) C.OffsetSyncDisable, C.OffsetStoreFile (Text.unpack name) C.OffsetSyncImmediate, C.OffsetStoreFile (Text.unpack name) (C.OffsetSyncInterval partition)]
               in QC.conjoin
                    [ allDistinct offsets,
                      allDistinct events,
                      allDistinct stores,
                      QC.conjoin (map generic offsets),
                      QC.conjoin (map generic events),
                      QC.conjoin (map generic stores),
                      generic C.OffsetCommit,
                      generic C.OffsetCommitAsync,
                      generic C.Earliest,
                      generic C.Latest,
                      generic (C.SubscribedPartitions (map snd partitions)),
                      generic C.SubscribedPartitionsAll,
                      readShow message,
                      readShow headers,
                      generic message,
                      generic failure,
                      (C.cmeError failure, C.cmeTopic failure, C.cmePartition failure, C.cmeOffset failure) QC.=== (K.KafkaInvalidReturnValue, Just topic, Just (K.PartitionId partition), Just (C.Offset offset))
                    ],
          QC.testProperty "producer records delivery variants and topic requests retain exact fields" $
            QC.forAll ((,,,,) <$> text <*> signed <*> signed64 <*> bytes <*> bytes) $ \(name, partition, offset, key, value) ->
              let topic = K.TopicName name
                  headers = K.headersFromList [(key, value)]
                  message = P.ProducerRecord topic (P.SpecifiedPartition partition) (Just key) (Just value) headers
                  reports = [P.DeliverySuccess message (C.Offset offset), P.DeliveryFailure message K.KafkaBadConfiguration, P.NoMessageError K.KafkaInvalidReturnValue]
                  request = T.NewTopic topic (T.PartitionCount partition) (T.ReplicationFactor partition) (M.singleton (Text.unpack name) (Text.unpack name))
               in QC.conjoin
                    [ (P.prTopic message, P.prPartition message, P.prKey message, P.prValue message, P.prHeaders message) QC.=== (topic, P.SpecifiedPartition partition, Just key, Just value, headers),
                      allDistinct reports,
                      QC.conjoin (map generic reports),
                      generic message,
                      QC.conjoin
                        [ (P.ImmediateError (K.KafkaError name) == P.ImmediateError (K.KafkaError (name <> "x"))) QC.=== False,
                          show (P.ImmediateError (K.KafkaError name)) QC.=== show (K.KafkaError name),
                          (case P.ImmediateError (K.KafkaError name) of P.ImmediateError err -> err) QC.=== K.KafkaError name
                        ],
                      QC.property (P.SpecifiedPartition partition /= P.UnassignedPartition),
                      map fromEnum [P.PurgeQueue, P.PurgeInFlight, P.PurgeNonBlocking] QC.=== [0, 1, 2],
                      [minBound .. maxBound :: P.PurgeFlag] QC.=== [P.PurgeQueue, P.PurgeInFlight, P.PurgeNonBlocking],
                      (T.topicName request, T.unPartitionCount (T.topicPartitionCount request), T.unReplicationFactor (T.topicReplicationFactor request), T.topicConfig request) QC.=== (topic, partition, partition, M.singleton (Text.unpack name) (Text.unpack name))
                    ],
          QC.testProperty "metadata structures retain identities states members and binary data" $
            QC.forAll ((,,,,,) <$> text <*> signed <*> signed <*> signed64 <*> bytes <*> bytes) $ \(name, n, port, offset, binary, assignment) ->
              let topic = K.TopicName name
                  brokerId = K.BrokerId n
                  broker = D.BrokerMetadata brokerId name port
                  partition = D.PartitionMetadata (K.PartitionId n) Nothing brokerId [brokerId] [brokerId]
                  topicInfo = D.TopicMetadata topic [partition] Nothing
                  metadata = D.KafkaMetadata [broker] [topicInfo] brokerId
                  watermark = D.WatermarkOffsets topic (K.PartitionId n) (C.Offset offset) (C.Offset offset)
                  member = D.GroupMemberInfo (D.GroupMemberId name) (K.ClientId name) name binary assignment
                  states = [D.GroupPreparingRebalance, D.GroupEmpty, D.GroupAwaitingSync, D.GroupStable, D.GroupDead]
                  group state = D.GroupInfo (C.ConsumerGroupId name) Nothing state (D.GroupProtocolType name) (D.GroupProtocol name) [member]
               in QC.conjoin
                    [ generic broker,
                      generic partition,
                      generic topicInfo,
                      generic metadata,
                      generic watermark,
                      generic member,
                      (D.bmBrokerId broker, D.bmBrokerHost broker, D.bmBrokerPort broker) QC.=== (brokerId, name, port),
                      (D.kmBrokers metadata, D.kmTopics metadata, D.kmOrigBroker metadata) QC.=== ([broker], [topicInfo], brokerId),
                      (D.gmiMemberId member, D.gmiClientId member, D.gmiClientHost member, D.gmiMetadata member, D.gmiAssignment member) QC.=== (D.GroupMemberId name, K.ClientId name, name, binary, assignment),
                      QC.conjoin (map (generic . group) states),
                      QC.conjoin (map readShow states),
                      allDistinct (map group states)
                    ],
          QC.testProperty "every Kafka error preserves its Exception identity and generic form" $
            QC.forAll text $ \message ->
              let errors = [K.KafkaError message, K.KafkaInvalidReturnValue, K.KafkaBadSpecification message, K.KafkaResponseError C.RdKafkaRespErrTimedOut, K.KafkaInvalidConfigurationValue message, K.KafkaUnknownConfigurationKey message, K.KafkaBadConfiguration]
               in QC.conjoin [QC.conjoin [fromException (toException error) QC.=== Just error, generic error, case error of K.KafkaResponseError _ -> QC.property True; _ -> displayException error QC.=== show error] | error <- errors]
        ],
      testCase "all linked native errors retain sparse numeric codes names and descriptions" $ do
        count <- errorCount
        assertBool "native error descriptor table is empty" (count > 0)
        forM_ [0 .. count - 1] $ \index -> do
          code <- fromIntegral <$> errorCode index
          name <- errorName index >>= peekCString
          description <- errorDescription index >>= peekCString
          let value = toEnum code :: C.RdKafkaRespErrT
          fromEnum value @?= code
          displayException (K.KafkaResponseError value) @?= "[" <> name <> "] " <> description
        forM_ (zip [0 .. 2] [C.RdKafkaRespErrBegin, C.RdKafkaRespErrEnd, C.RdKafkaRespErrEndAll]) $ \(index, value) -> do
          code <- fromIntegral <$> sentinel index
          fromEnum value @?= code
          (toEnum code :: C.RdKafkaRespErrT) @?= value
    ]

consumer :: C.ConsumerProperties -> (M.Map Text.Text Text.Text, Maybe K.KafkaLogLevel, Maybe C.CallbackPollMode, Int)
consumer p = (C.cpProps p, C.cpLogLevel p, C.cpCallbackPollMode p, length (C.cpCallbacks p))

producer :: P.ProducerProperties -> (M.Map Text.Text Text.Text, M.Map Text.Text Text.Text, Maybe K.KafkaLogLevel, Int)
producer p = (P.ppKafkaProps p, P.ppTopicProps p, P.ppLogLevel p, length (P.ppCallbacks p))

readShow :: (Eq a, Show a, Read a) => a -> QC.Property
readShow value = QC.conjoin [read (show value) QC.=== value, read (show [value]) QC.=== [value]]

generic :: (Eq a, Show a, G.Generic a) => a -> QC.Property
generic value = G.to (G.from value) QC.=== value

allDistinct :: (Eq a, Show a) => [a] -> QC.Property
allDistinct values = QC.conjoin [QC.counterexample (show (a, b)) (QC.property (a /= b)) | (i, a) <- zip [0 :: Int ..] values, (j, b) <- zip [0 :: Int ..] values, i /= j]

text :: QC.Gen Text.Text
text = QC.frequency [(2, smallText), (5, Text.pack <$> boundedList 30 QC.arbitrary)]

strings :: QC.Gen [Text.Text]
strings = boundedList 8 text

bytes :: QC.Gen BS.ByteString
bytes = BS.pack <$> boundedList 30 QC.arbitrary

signed :: QC.Gen Int
signed = QC.frequency [(1, QC.elements [minBound, -1, 0, 1, maxBound]), (4, QC.arbitrary)] :: QC.Gen Int

signed64 :: QC.Gen Int64
signed64 = QC.frequency [(1, QC.elements [minBound, -1, 0, 1, maxBound]), (4, QC.arbitrary)] :: QC.Gen Int64

codecs :: [(K.KafkaCompressionCodec, Text.Text)]
codecs = [(K.NoCompression, "none"), (K.Gzip, "gzip"), (K.Snappy, "snappy"), (K.Lz4, "lz4"), (K.Zstd, "zstd")]

contexts :: [(K.KafkaDebug, String)]
contexts = [(K.DebugGeneric, "generic"), (K.DebugBroker, "broker"), (K.DebugTopic, "topic"), (K.DebugMetadata, "metadata"), (K.DebugQueue, "queue"), (K.DebugMsg, "msg"), (K.DebugProtocol, "protocol"), (K.DebugCgrp, "cgrp"), (K.DebugSecurity, "security"), (K.DebugFetch, "fetch"), (K.DebugFeature, "feature"), (K.DebugAll, "all")]
