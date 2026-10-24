module Kafka.Integration.ConsumerPropertyTest (tests) where

import Control.Concurrent.MVar (newEmptyMVar, tryPutMVar, tryReadMVar)
import Control.Exception (bracket, finally, throwIO)
import Control.Monad (void)
import Data.Int (Int64)
import Data.Text qualified as Text
import Kafka.Consumer
import Kafka.FixtureSupport (acquirePair)
import Kafka.Producer qualified as P
import Kafka.PropertySupport
import Kafka.TestEnv (TestEnv, makeGroupId, produceTo, releaseConsumer, releaseProducer, teBrokers, withTopic)
import Kafka.Transaction qualified as T
import Kafka.TransactionTestSupport
import System.Mem (performMajorGC)
import System.Timeout (timeout)
import Test.Tasty (TestTree, withResource)
import Test.Tasty.HUnit (assertFailure, testCase, (@?=))
import Test.Tasty.QuickCheck qualified as QC

tests :: TestEnv -> TestTree
tests testEnv =
  generatedGroup
    "consumer-properties"
    $ [ QC.testProperty "consumer callback composition preserves registration order and identity" $
          QC.forAllShrink programs shrinkPrograms $ \input@(a, b, c, leftAssociated, mode) -> QC.ioProperty $ do
            result <- observe a b c leftAssociated mode
            performMajorGC
            let labels = a <> b <> c
                expected = case reverse labels of [] -> Nothing; value : _ -> Just value
            pure (QC.counterexample (show input) (result QC.=== (length labels, expected))),
        withResource offsetClients releaseOffsetClients $ \clients ->
          QC.testProperty "generated source offsets reach native code as the representable next position" $
            QC.forAllShrink nextOffset (filter (>= 0) . QC.shrink) $ \n -> QC.ioProperty $ do
              (producer, consumer) <- clients
              withTxFault 100 $ do
                let message = ConsumerRecord (TopicName "generated-source-offset") (PartitionId 0) (Offset n) NoTimestamp mempty () ()
                result <- T.commitOffsetMessageTransaction producer consumer message (Timeout 1000)
                actualOffset <- txStat 12
                created <- txStat 0
                destroyed <- txStat 1
                metadata <- txStat 6
                freedMetadata <- txStat 7
                afterFree <- txStat 8
                pure $
                  (fmap T.getKafkaError result, actualOffset, created, destroyed, metadata, freedMetadata, afterFree)
                    QC.=== (Just (KafkaResponseError RdKafkaRespErrState), n + 1, 1, 1, 1, 1, 0)
      ]
      <> [ testCase (label <> " later subscription reset reaches the native consumer") $
             withTopic testEnv (Text.pack ("subscription-last-" <> label)) 1 $ \topic producer -> do
               produceTo producer topic 0 "existing"
               let props = brokersList (teBrokers testEnv) <> groupId (makeGroupId testEnv (Text.pack ("subscription-last-" <> label))) <> noAutoCommit <> callbackPollMode mode
               bracket (newConsumer props (topics [topic] <> offsetReset Latest <> offsetReset Earliest) >>= either throwIO pure) releaseConsumer $ \consumer -> do
                 record <- pollMessage consumer (Timeout 10000) >>= either throwIO pure
                 (crOffset record, crValue record) @?= (Offset 0, Just "existing")
         | (label, mode) <- [("async", CallbackPollModeAsync), ("sync", CallbackPollModeSync)]
         ]

-- Public records consumed from Kafka have nonnegative offsets; generation is
-- restricted to offsets whose next position fits the native signed field.
nextOffset :: QC.Gen Int64
nextOffset = QC.frequency [(1, QC.elements [0, 1, maxBound - 1]), (4, QC.choose (0, maxBound - 1))]

offsetClients :: IO (P.KafkaProducer, KafkaConsumer)
offsetClients =
  acquirePair
    (P.newProducer (P.brokersList ["127.0.0.1:1"] <> P.extraProp "log_level" "0") >>= either throwIO pure)
    releaseProducer
    (newAssignedConsumer (brokersList ["127.0.0.1:1"] <> groupId "generated-offset-unused-group" <> callbackPollMode CallbackPollModeSync <> extraProp "log_level" "0") >>= either throwIO pure)

releaseOffsetClients :: (P.KafkaProducer, KafkaConsumer) -> IO ()
releaseOffsetClients (producer, consumer) = destroyConsumer consumer `finally` releaseProducer producer

programs :: QC.Gen ([Int], [Int], [Int], Bool, CallbackPollMode)
programs = (,,,,) <$> labels <*> labels <*> labels <*> QC.arbitrary <*> QC.elements [CallbackPollModeAsync, CallbackPollModeSync]
  where
    labels = boundedList 6 (QC.choose (0, 5))

shrinkPrograms :: ([Int], [Int], [Int], Bool, CallbackPollMode) -> [([Int], [Int], [Int], Bool, CallbackPollMode)]
shrinkPrograms (a, b, c, association, mode) =
  [(a', b, c, association, mode) | a' <- QC.shrinkList QC.shrink a]
    <> [(a, b', c, association, mode) | b' <- QC.shrinkList QC.shrink b]
    <> [(a, b, c', association, mode) | c' <- QC.shrinkList QC.shrink c]
    <> [(a, b, c, False, mode) | association]

{-# NOINLINE observe #-}
observe :: [Int] -> [Int] -> [Int] -> Bool -> CallbackPollMode -> IO (Int, Maybe Int)
observe a b c association mode = do
  called <- newEmptyMVar
  let callback label = logCallback (\_ _ _ -> void (tryPutMVar called label))
      callbacks = foldMap (setCallback . callback . Just)
      p = callbacks a
      q = callbacks b
      r = callbacks c
      composed = if association then ((p <> mempty) <> q) <> r else p <> (mempty <> (q <> r))
      props =
        brokersList [BrokerAddress "127.0.0.1:1"]
          <> groupId (ConsumerGroupId "consumer-property-unused-group")
          <> extraProp "debug" "generic"
          <> extraProp "log_level" "7"
          <> callbackPollMode mode
          <> setCallback (callback Nothing)
          <> composed
  bracket (newAssignedConsumer props >>= either throwIO pure) destroyConsumer $ \consumer -> do
    let poll =
          tryReadMVar called >>= \case
            Just value -> pure value
            Nothing -> pollConsumerEvents consumer (Just (Timeout 50)) >> poll
    label <- timeout 5000000 poll >>= maybe (assertFailure "generated consumer callback did not run") pure
    pure (length (cpCallbacks composed), label)
