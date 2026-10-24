{-# OPTIONS_GHC -Wno-deprecations #-}

module Kafka.PublicPropertiesSpec (tests) where

import Control.Monad (forM_)
import Data.Bifoldable (bifoldMap)
import Data.Bifunctor (bimap, first, second)
import Data.Bitraversable (bitraverse)
import Data.ByteString qualified as BS
import Data.Functor.Compose (Compose (..))
import Data.Functor.Const (Const (..))
import Data.Functor.Identity (Identity (..))
import Data.Int (Int64)
import Data.List (nub, sort)
import Data.Map.Strict qualified as M
import Data.Set qualified as Set
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Word (Word32, Word8)
import Kafka.PropertySupport
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (testCase, (@?=))
import Test.Tasty.QuickCheck qualified as QC
import "rdkafka-hs" Kafka.Consumer qualified as C
import "rdkafka-hs" Kafka.Types qualified as K
import "rdkafka-hs" Kafka.Version qualified as V

data Edit = Property Text Text | Level K.KafkaLogLevel | Mode C.CallbackPollMode
  deriving stock (Eq, Show)

data SubEdit = SubTopic Text | SubProperty Text Text | Reset C.OffsetReset
  deriving stock (Eq, Show)

tests :: TestTree
tests =
  testGroup
    "Public properties"
    [ generatedGroup
        "consumer composition"
        [ QC.testProperty "consumer program follows independent last-write model" $
            QC.forAllShrink program shrinkProgram $
              \edits -> view (assemble edits) QC.=== model edits,
          QC.testProperty "consumer identity preserves every observable field" $
            QC.forAllShrink program shrinkProgram $ \edits ->
              let p = assemble edits in QC.conjoin [view (mempty <> p) QC.=== view p, view (p <> mempty) QC.=== view p],
          QC.testProperty "consumer association preserves writes and poll mode" $
            QC.forAll ((,,) <$> program <*> program <*> program) $ \(a, b, c) ->
              view ((assemble a <> assemble b) <> assemble c) QC.=== view (assemble a <> (assemble b <> assemble c)),
          QC.testProperty "consumer composition appends programs" $
            QC.forAll ((,) <$> program <*> program) $
              \(a, b) -> view (assemble a <> assemble b) QC.=== model (a <> b),
          QC.testProperty "unset poll mode preserves a prior explicit mode" $
            QC.forAll ((,,) <$> mode <*> smallText <*> smallText) $ \(poll, key, value) ->
              C.effectiveCallbackPollMode (C.callbackPollMode poll <> C.extraProp key value) QC.=== poll,
          QC.testProperty "consumer helpers write exact native property names" $
            QC.forAll ((,,) <$> smallText <*> smallText <*> QC.arbitrary) $ \(group, client, millis :: Int64) ->
              C.cpProps (C.groupId (C.ConsumerGroupId group) <> C.clientId (K.ClientId client) <> C.autoCommit (K.Millis millis) <> C.noAutoOffsetStore)
                QC.=== M.fromList [("group.id", group), ("client.id", client), ("enable.auto.commit", "true"), ("auto.commit.interval.ms", Text.pack (show millis)), ("enable.auto.offset.store", "false")]
        ],
      generatedGroup
        "subscription"
        [ QC.testProperty "subscription follows last-write properties and topic union" $
            QC.forAllShrink subscriptionProgram shrinkSubscription $
              \edits -> subscriptionView (subscription edits) QC.=== subscriptionModel edits,
          QC.testProperty "subscription identity and association retain topics and properties" $
            QC.forAll ((,,) <$> subscriptionProgram <*> subscriptionProgram <*> subscriptionProgram) $ \(a, b, c) ->
              let x = subscription a; y = subscription b; z = subscription c
               in QC.conjoin [subscriptionView (mempty <> x) QC.=== subscriptionView x, subscriptionView (x <> mempty) QC.=== subscriptionView x, subscriptionView ((x <> y) <> z) QC.=== subscriptionView (x <> (y <> z))],
          QC.testProperty "subscription composition appends property writes" $
            QC.forAll ((,) <$> subscriptionProgram <*> subscriptionProgram) $
              \(a, b) -> subscriptionView (subscription a <> subscription b) QC.=== subscriptionModel (a <> b),
          testCase "later subscription reset and explicit properties win" $ do
            subscriptionView (C.offsetReset C.Latest <> C.offsetReset C.Earliest) @?= ([], M.singleton "auto.offset.reset" "earliest")
            subscriptionView (C.extraSubscriptionProps (M.singleton "shared" "before") <> C.extraSubscriptionProps (M.singleton "shared" "after")) @?= ([], M.singleton "shared" "after")
            subscriptionView (C.topics ["a", "b"] <> C.topics ["b", "c"]) @?= (["a", "b", "c"], M.empty)
        ],
      generatedGroup
        "headers"
        [ QC.testProperty "header list round-trip preserves duplicates and binary bytes" $
            QC.forAllShrink headerPairs shrinkPairs $
              \pairs -> K.headersToList (K.headersFromList pairs) QC.=== pairs,
          QC.testProperty "header concatenation follows list order" $
            QC.forAll ((,) <$> headerPairs <*> headerPairs) $ \(a, b) ->
              K.headersToList (K.headersFromList a <> K.headersFromList b) QC.=== (a <> b),
          QC.testProperty "headers have two identities and associative concatenation" $
            QC.forAll ((,,) <$> headerPairs <*> headerPairs <*> headerPairs) $ \(a, b, c) ->
              let x = K.headersFromList a; y = K.headersFromList b; z = K.headersFromList c
               in QC.conjoin [mempty <> x QC.=== x, x <> mempty QC.=== x, (x <> y) <> z QC.=== x <> (y <> z)],
          testCase "fixed duplicate empty and binary header values" $ do
            let pairs = [("same", ""), ("same", BS.pack [0, 255]), ("", ""), ("same", "last")]
            K.headersToList (K.headersFromList pairs) @?= pairs
        ],
      generatedGroup
        "records"
        [ law "record functor identity" $ \r -> fmap id r QC.=== r,
          law "record functor composition" $ \r -> fmap ((+ 1) . (* 3)) r QC.=== (fmap (+ 1) . fmap (* 3)) r,
          law "record bifunctor identity and composition" $ \r ->
            QC.conjoin
              [ bimap id id r QC.=== r,
                bimap ((+ 1) . (* 2)) (subtract 1 . (* 3)) r QC.=== (bimap (+ 1) (subtract 1) . bimap (* 2) (* 3)) r
              ],
          law "record mapping preserves origin timestamp and headers" $ \r ->
            let mapped = bimap (+ 1) (* 3) r
             in (C.crTopic mapped, C.crPartition mapped, C.crOffset mapped, C.crTimestamp mapped, C.crHeaders mapped, C.crKey mapped, C.crValue mapped)
                  QC.=== (C.crTopic r, C.crPartition r, C.crOffset r, C.crTimestamp r, C.crHeaders r, C.crKey r + 1, C.crValue r * 3),
          law "record traversal identity and failure preserve the applicative contract" $ \r ->
            QC.conjoin
              [ runIdentity (traverse Identity r) QC.=== r,
                traverse Just r QC.=== Just r,
                traverse (const (Nothing :: Maybe Int)) r QC.=== Nothing,
                getConst (traverse (const (Const ["value" :: String])) r) QC.=== ["value"]
              ],
          law "record traversal composition preserves nested effects" $ \r ->
            let f value = if value `mod` 3 == 0 then Nothing else Just (value + 1)
                g value = [value, value * 2]
             in getCompose (traverse (Compose . fmap g . f) r) QC.=== fmap (traverse g) (traverse f r),
          law "record bitraversal visits key before value" $ \r ->
            QC.conjoin
              [ runIdentity (bitraverse Identity Identity r) QC.=== r,
                bitraverse Just Just r QC.=== Just r,
                bitraverse (const (Nothing :: Maybe Int)) Just r QC.=== Nothing,
                bitraverse Just (const (Nothing :: Maybe Int)) r QC.=== Nothing,
                getConst (bitraverse (const (Const ["key" :: String])) (const (Const ["value"])) r) QC.=== ["key", "value"]
              ],
          law "record bitraversal composition preserves nested effects" $ \r ->
            let f value = if value `mod` 3 == 0 then Nothing else Just (value + 1)
                g value = [value, value * 2]
                h value = [value - 1, value]
             in getCompose (bitraverse (Compose . fmap g . f) (Compose . fmap h . f) r) QC.=== fmap (bitraverse g h) (bitraverse f f r),
          law "record folds retain key and value order" $ \r ->
            QC.conjoin
              [ foldMap (: []) r QC.=== [C.crValue r],
                bifoldMap (\x -> [("key" :: String, x)]) (\x -> [("value", x)]) r QC.=== [("key", C.crKey r), ("value", C.crValue r)]
              ],
          law "deprecated record mapping remains equivalent" $ \r ->
            QC.conjoin
              [ C.crMapKey (+ 1) r QC.=== first (+ 1) r,
                C.crMapValue (* 3) r QC.=== second (* 3) r,
                C.crMapKV (+ 1) (* 3) r QC.=== bimap (+ 1) (* 3) r
              ],
          law "deprecated traversal helpers preserve successful effects" $ \r ->
            QC.conjoin
              [ C.sequenceFirst (first Just r) QC.=== Just r,
                C.traverseFirst Just r QC.=== Just r,
                runIdentity (C.traverseFirstM (Identity . Just) r) QC.=== Just r,
                runIdentity (C.traverseM (Identity . Just) r) QC.=== Just r,
                runIdentity (C.bitraverseM (Identity . Just) (Identity . Just) r) QC.=== Just r
              ],
          law "deprecated traversal helpers preserve failed effects" $ \r ->
            QC.conjoin
              [ C.traverseFirst (const (Nothing :: Maybe Int)) r QC.=== Nothing,
                runIdentity (C.traverseFirstM (const (Identity (Nothing :: Maybe Int))) r) QC.=== Nothing,
                runIdentity (C.traverseM (const (Identity (Nothing :: Maybe Int))) r) QC.=== Nothing,
                runIdentity (C.bitraverseM (Identity . Just) (const (Identity (Nothing :: Maybe Int))) r) QC.=== Nothing
              ]
        ],
      generatedGroup
        "representations"
        [ QC.testProperty "text newtypes preserve arbitrary represented text" $
            QC.forAll smallText $ \text ->
              QC.conjoin
                [K.unTopicName (K.TopicName text) QC.=== text, K.unClientId (K.ClientId text) QC.=== text, K.unBrokerAddress (K.BrokerAddress text) QC.=== text, C.unConsumerGroupId (C.ConsumerGroupId text) QC.=== text],
          QC.testProperty "integer newtypes preserve signed machine boundaries" $ \(n :: Int) ->
            QC.conjoin
              [K.unBrokerId (K.BrokerId n) QC.=== n, K.unPartitionId (K.PartitionId n) QC.=== n, K.unBatchSize (K.BatchSize n) QC.=== n, K.unTimeout (K.Timeout n) QC.=== n],
          QC.testProperty "offset and milliseconds preserve signed 64-bit boundaries" $ \(n :: Int64) ->
            QC.conjoin
              [C.unOffset (C.Offset n) QC.=== n, K.unMillis (K.Millis n) QC.=== n, read (show (C.Offset n)) QC.=== C.Offset n, read (show (K.Millis n)) QC.=== K.Millis n],
          QC.testProperty "timestamp constructors retain their represented boundary" $
            QC.forAll timestamp $
              \value -> (read (show value) :: C.Timestamp) QC.=== value,
          QC.testProperty "system topic classification requires two initial underscores" $
            QC.forAll smallText $
              \text -> K.topicType (K.TopicName text) QC.=== (case Text.unpack text of '_' : '_' : _ -> K.System; _ -> K.User),
          testCase "fixed empty underscore and signed numeric boundaries" $ do
            map (K.topicType . K.TopicName) ["", "_", "__", "___", "x__", "__λ"] @?= [K.User, K.User, K.System, K.System, K.User, K.System]
            forM_ [minBound, 0, maxBound :: Int64] $ \n -> do
              C.unOffset (C.Offset n) @?= n
              (read (show (C.CreateTime (K.Millis n))) :: C.Timestamp) @?= C.CreateTime (K.Millis n)
        ],
      generatedGroup
        "version"
        [ QC.testProperty "version bytes decode independently of prerelease byte" $ \(major :: Word8) (minor :: Word8) (revision :: Word8) (pre :: Word8) ->
            let packed = (((fromIntegral major * 256 + fromIntegral minor) * 256 + fromIntegral revision) * 256 + fromIntegral pre) :: Word32
             in V.decodeLibrdkafkaVersion (fromIntegral packed) QC.=== V.LibrdkafkaVersion (fromIntegral major) (fromIntegral minor) (fromIntegral revision),
          QC.testProperty "version extraction agrees with arithmetic for every Int" $ \(n :: Int) ->
            V.decodeLibrdkafkaVersion n QC.=== V.LibrdkafkaVersion ((n `div` 16777216) `mod` 256) ((n `div` 65536) `mod` 256) ((n `div` 256) `mod` 256),
          testCase "fixed zero and full version byte boundaries" $ do
            V.decodeLibrdkafkaVersion 0 @?= V.LibrdkafkaVersion 0 0 0
            V.decodeLibrdkafkaVersion (fromIntegral (maxBound :: Word32)) @?= V.LibrdkafkaVersion 255 255 255
        ],
      testCase "every compression debug and log enumeration has its native spelling" $ do
        map K.kafkaCompressionCodecToText [K.NoCompression, K.Gzip, K.Snappy, K.Lz4, K.Zstd] @?= ["none", "gzip", "snappy", "lz4", "zstd"]
        map K.kafkaDebugToText [K.DebugGeneric, K.DebugBroker, K.DebugTopic, K.DebugMetadata, K.DebugQueue, K.DebugMsg, K.DebugProtocol, K.DebugCgrp, K.DebugSecurity, K.DebugFetch, K.DebugFeature, K.DebugAll]
          @?= ["generic", "broker", "topic", "metadata", "queue", "msg", "protocol", "cgrp", "security", "fetch", "feature", "all"]
        map fromEnum [K.KafkaLogEmerg .. K.KafkaLogDebug] @?= [0 .. 7]
    ]

program :: QC.Gen [Edit]
program = boundedList 30 (QC.oneof [Property <$> smallText <*> smallText, Level <$> QC.elements [K.KafkaLogEmerg .. K.KafkaLogDebug], Mode <$> mode])

mode :: QC.Gen C.CallbackPollMode
mode = QC.elements [C.CallbackPollModeAsync, C.CallbackPollModeSync]

shrinkProgram :: [Edit] -> [[Edit]]
shrinkProgram = QC.shrinkList $ \case
  Property key value -> [Property key' value | key' <- shrinkText key] <> [Property key value' | value' <- shrinkText value]
  _ -> []

assemble :: [Edit] -> C.ConsumerProperties
assemble = foldMap $ \case
  Property key value -> C.extraProp key value
  Level level -> C.logLevel level
  Mode poll -> C.callbackPollMode poll

view :: C.ConsumerProperties -> (M.Map Text Text, Maybe K.KafkaLogLevel, Maybe C.CallbackPollMode, C.CallbackPollMode)
view p = (C.cpProps p, C.cpLogLevel p, C.cpCallbackPollMode p, C.effectiveCallbackPollMode p)

model :: [Edit] -> (M.Map Text Text, Maybe K.KafkaLogLevel, Maybe C.CallbackPollMode, C.CallbackPollMode)
model edits =
  let (props, level, poll) = foldl' step (M.empty, Nothing, Nothing) edits
   in (props, level, poll, maybe C.CallbackPollModeAsync id poll)
  where
    step (props, level, poll) = \case
      Property key value -> (M.insert key value props, level, poll)
      Level value -> (props, Just value, poll)
      Mode value -> (props, level, Just value)

bytes :: QC.Gen BS.ByteString
bytes = BS.pack <$> boundedList 20 QC.arbitrary

headerPairs :: QC.Gen [(BS.ByteString, BS.ByteString)]
headerPairs = boundedList 15 ((,) <$> QC.oneof [pure "same", bytes] <*> bytes)

shrinkPairs :: [(BS.ByteString, BS.ByteString)] -> [[(BS.ByteString, BS.ByteString)]]
shrinkPairs = QC.shrinkList $ \(key, value) -> [(BS.pack k, value) | k <- QC.shrink (BS.unpack key)] <> [(key, BS.pack v) | v <- QC.shrink (BS.unpack value)]

signed64 :: QC.Gen Int64
signed64 = QC.frequency [(1, QC.elements [minBound, -2002, -1001, -2, -1, 0, 1, maxBound]), (4, QC.arbitrary)]

timestamp :: QC.Gen C.Timestamp
timestamp = QC.oneof [pure C.NoTimestamp, C.CreateTime . K.Millis <$> signed64, C.LogAppendTime . K.Millis <$> signed64]

record :: QC.Gen (C.ConsumerRecord Int Int)
record = C.ConsumerRecord <$> (K.TopicName <$> smallText) <*> (K.PartitionId <$> QC.arbitrary) <*> (C.Offset <$> signed64) <*> timestamp <*> (K.headersFromList <$> headerPairs) <*> QC.arbitrary <*> QC.arbitrary

law :: String -> (C.ConsumerRecord Int Int -> QC.Property) -> TestTree
law name = QC.testProperty name . QC.forAllShrink record shrinkRecord
  where
    shrinkRecord r =
      [r {C.crKey = k} | k <- QC.shrink (C.crKey r)]
        <> [r {C.crValue = v} | v <- QC.shrink (C.crValue r)]
        <> [r {C.crHeaders = K.headersFromList pairs} | pairs <- shrinkPairs (K.headersToList (C.crHeaders r))]
        <> [r {C.crOffset = C.Offset n} | n <- QC.shrink (C.unOffset (C.crOffset r))]
        <> [r {C.crTopic = K.TopicName text} | text <- shrinkText (K.unTopicName (C.crTopic r))]
        <> [r {C.crPartition = K.PartitionId n} | n <- QC.shrink (K.unPartitionId (C.crPartition r))]
        <> [r {C.crTimestamp = C.NoTimestamp} | C.crTimestamp r /= C.NoTimestamp]

subscriptionProgram :: QC.Gen [SubEdit]
subscriptionProgram = boundedList 30 (QC.oneof [SubTopic <$> smallText, SubProperty <$> smallText <*> smallText, Reset <$> QC.elements [C.Earliest, C.Latest]])

shrinkSubscription :: [SubEdit] -> [[SubEdit]]
shrinkSubscription = QC.shrinkList $ \case
  SubTopic topic -> [SubTopic value | value <- shrinkText topic]
  SubProperty key value -> [SubProperty key' value | key' <- shrinkText key] <> [SubProperty key value' | value' <- shrinkText value]
  Reset _ -> []

subscription :: [SubEdit] -> C.Subscription
subscription = foldMap $ \case
  SubTopic value -> C.topics [K.TopicName value]
  SubProperty key value -> C.extraSubscriptionProps (M.singleton key value)
  Reset value -> C.offsetReset value

subscriptionView :: C.Subscription -> ([K.TopicName], M.Map Text Text)
subscriptionView (C.Subscription topics props) = (Set.toAscList topics, props)

subscriptionModel :: [SubEdit] -> ([K.TopicName], M.Map Text Text)
subscriptionModel edits = let (topics, props) = foldl' step ([], M.empty) edits in (sort (nub topics), props)
  where
    step (topics, props) = \case
      SubTopic value -> (topics <> [K.TopicName value], props)
      SubProperty key value -> (topics, M.insert key value props)
      Reset value -> (topics, M.insert "auto.offset.reset" (case value of C.Earliest -> "earliest"; C.Latest -> "latest") props)
