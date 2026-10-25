module Kafka.Integration.ProducerRegressionTest (tests, runCallbackProbe) where

import Control.Concurrent (forkFinally, killThread, runInBoundThread, threadDelay)
import Control.Concurrent.MVar (MVar, newEmptyMVar, putMVar, readMVar, takeMVar, tryPutMVar, tryReadMVar)
import Control.Exception (AsyncException (HeapOverflow, ThreadKilled), MaskingState (Unmasked), SomeException, bracket, evaluate, finally, fromException, getMaskingState, throwIO, try)
import Control.Monad (forM_, replicateM, void)
import Data.ByteString qualified as BS
import Data.IORef (IORef, atomicModifyIORef', mkWeakIORef, newIORef, readIORef)
import Data.List (isInfixOf)
import Data.Map qualified as Map
import Data.Text qualified as Text
import Foreign.C.Types (CInt)
import GHC.Conc (BlockReason (BlockedOnException), ThreadStatus (ThreadBlocked), getUncaughtExceptionHandler, setUncaughtExceptionHandler, threadStatus)
import Kafka.CallbackTestSupport (assertRetained, awaitCollected)
import Kafka.Consumer qualified as C
import Kafka.Metadata (KafkaMetadata (..), PartitionMetadata (..), TopicMetadata (..), topicMetadata)
import Kafka.Producer
import Kafka.ProducerTestSupport
import Kafka.TestEnv (TestEnv, consumerProps, makeGroupId, prefixedName, producerProps, receiveMessages, releaseConsumer, releaseProducer, sendMessages, withTopic)
import System.Directory (createDirectoryIfMissing)
import System.Environment (getEnvironment, getExecutablePath, lookupEnv)
import System.Exit (ExitCode (ExitSuccess))
import System.Mem.Weak (Weak)
import System.Process (CreateProcess (env), proc, readCreateProcessWithExitCode)
import System.Random (randomIO)
import System.Timeout (timeout)
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertBool, assertFailure, testCase, (@?=))

tests :: TestEnv -> TestTree
tests env =
  testGroup
    "producer-regressions"
    $ [ testCase "explicit topic timeout expires an accepted message" $
          checkTimeout (sendTimeout (Timeout 1000)),
        testCase "global timeout still expires an accepted message" $
          checkTimeout (extraProp "message.timeout.ms" "1000"),
        testCase "queue-full rejection releases callback capture without invoking it" $
          withProducer (extraProp "queue.buffering.max.messages" "1") $ \producer -> do
            produceMessage producer record >>= (@?= Nothing)
            calls <- newIORef 0
            (result, weak) <- enqueueCapture producer record calls Nothing
            result @?= Left (ImmediateError (KafkaResponseError RdKafkaRespErrQueueFull))
            awaitCollected weak
            readIORef calls >>= (@?= 0),
        testCase "accepted callback retains its capture until a purged report completes" $
          withProducer mempty $ \producer -> do
            calls <- newIORef 0
            reports <- newEmptyMVar
            (result, weak) <- enqueueCapture producer record calls (Just reports)
            result @?= Right ()
            assertRetained weak
            purgeProducer producer [PurgeQueue] >>= (@?= Nothing)
            flushProducerTimeout producer (Timeout 1000) >>= (@?= Nothing)
            takeWithin reports >>= (@?= DeliveryFailure record (KafkaResponseError RdKafkaRespErrPurgeQueue))
            awaitCollected weak
            readIORef calls >>= (@?= 1),
        testCase "throwing global callback preserves per-message delivery and releases capture" $
          callbackChild "global",
        testCase "throwing per-message callback reports exception and releases capture" $
          callbackChild "message",
        testCase "pinned argument allocation failure releases callback before native enqueue" $
          callbackChild "allocation",
        testCase "repeated queue-full rejections release every callback capture" $
          withProducer (extraProp "queue.buffering.max.messages" "1") $ \producer -> do
            produceMessage producer record >>= (@?= Nothing)
            calls <- newIORef 0
            captures <- replicateM 50 $ do
              (result, weak) <- enqueueCapture producer record calls Nothing
              result @?= Left (ImmediateError (KafkaResponseError RdKafkaRespErrQueueFull))
              pure weak
            mapM_ awaitCollected captures
            readIORef calls >>= (@?= 0),
        testCase "cancellation at a rejected enqueue releases capture before unmasking" $
          withProducer mempty $ \producer -> withProducerTracking 1 $ do
            weakReady <- newEmptyMVar
            completed <- newEmptyMVar
            reports <- newEmptyMVar
            calls <- newIORef 0
            worker <- forkFinally (enqueueObservedCapture producer record calls weakReady reports) (putMVar completed)
            flip finally (releaseEnqueueBoundary >> killThread worker) $ do
              awaitEnqueueBoundary
              weak <- takeWithin weakReady
              assertRetained weak
              killed <- newEmptyMVar
              killer <- forkFinally (killThread worker) (putMVar killed)
              awaitCondition ((== ThreadBlocked BlockedOnException) <$> threadStatus killer)
              releaseEnqueueBoundary
              takeWithin completed >>= \case
                Left exception -> fromException exception @?= Just ThreadKilled
                Right value -> assertFailure ("cancellation failed: " <> show value)
              takeWithin killed >>= either throwIO pure
              awaitCollected weak
              readIORef calls >>= (@?= 0)
              flushProducerTimeout producer (Timeout 0) >>= (@?= Nothing),
        testCase "cancellation after native acceptance retains callback until its report" $
          withProducer mempty $ \producer -> withProducerTracking 2 $ do
            weakReady <- newEmptyMVar
            reports <- newEmptyMVar
            completed <- newEmptyMVar
            calls <- newIORef 0
            worker <- forkFinally (enqueueObservedCapture producer record calls weakReady reports) (putMVar completed)
            flip finally (releaseEnqueueBoundary >> killThread worker) $ do
              awaitEnqueueBoundary
              weak <- takeWithin weakReady
              assertRetained weak
              killed <- newEmptyMVar
              killer <- forkFinally (killThread worker) (putMVar killed)
              awaitCondition ((== ThreadBlocked BlockedOnException) <$> threadStatus killer)
              releaseEnqueueBoundary
              takeWithin completed >>= \case
                Left exception -> fromException exception @?= Just ThreadKilled
                Right value -> assertFailure ("cancellation failed: " <> show value)
              takeWithin killed >>= either throwIO pure
              assertRetained weak
              purgeProducer producer [PurgeQueue] >>= (@?= Nothing)
              flushProducerTimeout producer (Timeout 1000) >>= (@?= Nothing)
              takeWithin reports >>= (@?= DeliveryFailure record (KafkaResponseError RdKafkaRespErrPurgeQueue))
              awaitCollected weak
              readIORef calls >>= (@?= 1),
        testCase "another poller can deliver before successful enqueue returns" $
          withTopic env "producer-transfer-race" 1 $ \topic producer -> withProducerTracking 2 $ do
            let actual = record {prTopic = topic}
            calls <- newIORef 0
            reports <- newEmptyMVar
            completed <- newEmptyMVar
            worker <- forkFinally (enqueueCapture producer actual calls (Just reports)) (putMVar completed)
            flip finally (releaseEnqueueBoundary >> killThread worker) $ do
              awaitEnqueueBoundary
              flushProducerTimeout producer (Timeout 5000) >>= (@?= Nothing)
              takeWithin reports >>= (@?= DeliverySuccess actual (C.Offset 0))
              tryReadMVar completed >>= \case
                Nothing -> pure ()
                Just _ -> assertFailure "enqueue caller passed the native barrier before release"
              releaseEnqueueBoundary
              takeWithin completed >>= \case
                Left exception -> throwIO exception
                Right (result, weak) -> do
                  result @?= Right ()
                  awaitCollected weak
              readIORef calls >>= (@?= 1),
        testCase "running callback keeps its capture after the native root is released" $
          withProducer mempty $ \producer -> do
            started <- newEmptyMVar
            done <- newEmptyMVar
            (weak, _) <- bracket newEmptyMVar (`putMVar` ()) $ \resume -> do
              weak <- enqueueBlockedCapture producer record started resume done
              purgeProducer producer [PurgeQueue] >>= (@?= Nothing)
              flushProducerTimeout producer (Timeout 1000) >>= (@?= Nothing)
              takeWithin started
              assertRetained weak
              flushProducerTimeout producer (Timeout 0) >>= (@?= Nothing)
              pure (weak, ())
            takeWithin done
            awaitCollected weak,
        testCase "native error decoding failure releases rejected callback capture" $
          withProducer mempty $ \producer -> withProducerTracking 3 $ do
            calls <- newIORef 0
            ready <- newEmptyMVar
            reports <- newEmptyMVar
            outcome <- try @SomeException (enqueueObservedCapture producer record calls ready reports)
            case outcome of
              Left exception -> assertBool (show exception) ("32767" `isInfixOf` show exception)
              Right result -> assertFailure ("undecodable native error returned " <> show result)
            weak <- takeWithin ready
            nativeProducerProperty 1 >>= (@?= Just "60000")
            awaitCollected weak
            flushProducerTimeout producer (Timeout 0) >>= (@?= Nothing)
            readIORef calls >>= (@?= 0),
        testCase "non-null native success-code error rejects and releases capture" $
          withProducer mempty $ \producer -> withProducerTracking 4 $ do
            calls <- newIORef 0
            (result, weak) <- enqueueCapture producer record calls Nothing
            result @?= Left (ImmediateError KafkaInvalidReturnValue)
            awaitCollected weak
            flushProducerTimeout producer (Timeout 0) >>= (@?= Nothing)
            readIORef calls >>= (@?= 0),
        testCase "unknown native error code is preserved instead of reading errno" $
          runInBoundThread $
            withProducer mempty $ \producer -> withProducerTracking 5 $ do
              calls <- newIORef 0
              (result, weak) <- enqueueCapture producer record calls Nothing
              result @?= Left (ImmediateError (KafkaResponseError RdKafkaRespErrUnknown))
              awaitCollected weak
              flushProducerTimeout producer (Timeout 0) >>= (@?= Nothing)
              readIORef calls >>= (@?= 0)
      ]
      <> [ testCase ("actual client configuration: " <> label) $
             withProducerRaw props $ \producer -> withProducerTracking 0 $ do
               produceMessage producer record >>= (@?= Nothing)
               nativeProducerProperty 1 >>= (@?= Just expected)
               forM_ extras $ \(index, value) -> nativeProducerProperty index >>= (@?= Just value)
         | (label, props, expected, extras) <-
             [ ("defaults", mempty, "300000", [(2, "none")]),
               ("global only", extraProp "message.timeout.ms" "1200", "1200", []),
               ("topic only", sendTimeout (Timeout 1300), "1300", []),
               ("disjoint maps", extraProp "client.id" "preserved-client" <> extraProp "acks" "1" <> sendTimeout (Timeout 1400), "1400", [(0, "preserved-client"), (3, "1")]),
               ("explicit topic wins duplicate", extraProp "message.timeout.ms" "60000" <> sendTimeout (Timeout 1500), "1500", []),
               ("last topic property wins", sendTimeout (Timeout 1600) <> sendTimeout (Timeout 1700), "1700", []),
               ("empty topic map preserves global", extraProp "message.timeout.ms" "1800" <> mempty, "1800", []),
               ("zero topic timeout is infinite", sendTimeout (Timeout 0), "0", []),
               ("maximum native topic timeout", sendTimeout (Timeout (fromIntegral (maxBound :: CInt))), show (maxBound :: CInt), []),
               ("explicit topic wins native timeout alias", extraProp "delivery.timeout.ms" "60000" <> sendTimeout (Timeout 2000), "2000", []),
               ("topic codec overrides global codec", compression Gzip <> topicCompression Lz4, "300000", [(2, "lz4")]),
               ("global codec survives unrelated topic keys", compression Gzip <> sendTimeout (Timeout 1900), "1900", [(2, "gzip")])
             ]
         ]
      <> [testCase ("byte-exact round trip with " <> show codec) (roundTrip env codec) | codec <- [NoCompression, Gzip, Snappy, Lz4, Zstd]]
      <> [ testCase ("queued purge flags: " <> label) $
             withProducer mempty $ \producer -> do
               calls <- newIORef 0
               reports <- newEmptyMVar
               (result, weak) <- enqueueCapture producer record calls (Just reports)
               result @?= Right ()
               purgeProducer producer flags >>= (@?= Nothing)
               if PurgeQueue `elem` flags
                 then pure ()
                 else do
                   flushProducerTimeout producer (Timeout 0) >>= (@?= Just (KafkaResponseError RdKafkaRespErrTimedOut))
                   assertRetained weak
                   purgeProducer producer [PurgeQueue] >>= (@?= Nothing)
               flushProducerTimeout producer (Timeout 1000) >>= (@?= Nothing)
               takeWithin reports >>= (@?= DeliveryFailure record (KafkaResponseError RdKafkaRespErrPurgeQueue))
               awaitCollected weak
               closeProducer producer
               closeProducer producer
               readIORef calls >>= (@?= 1)
         | (label, flags) <- purgeCases
         ]
      <> [ testCase ("in-flight purge flags: " <> label) $
             withMockProducer $ \mock producer -> do
               calls <- newIORef 0
               reports <- newEmptyMVar
               delayMockProduce mock 10000
               (result, weak) <- enqueueCapture producer mockRecord calls (Just reports)
               result @?= Right ()
               awaitCondition ((== 1) <$> mockReceivedProduce mock)
               flushProducerTimeout producer (Timeout 0) >>= (@?= Just (KafkaResponseError RdKafkaRespErrTimedOut))
               purgeProducer producer flags >>= (@?= Nothing)
               if PurgeInFlight `elem` flags
                 then pure ()
                 else do
                   flushProducerTimeout producer (Timeout 0) >>= (@?= Just (KafkaResponseError RdKafkaRespErrTimedOut))
                   assertRetained weak
                   purgeProducer producer [PurgeInFlight] >>= (@?= Nothing)
               flushProducerTimeout producer (Timeout 1000) >>= (@?= Nothing)
               takeWithin reports >>= (@?= DeliveryFailure mockRecord (KafkaResponseError RdKafkaRespErrPurgeInflight))
               awaitCollected weak
               flushProducerTimeout producer (Timeout 0) >>= (@?= Nothing)
               readIORef calls >>= (@?= 1)
         | (label, flags) <- purgeCases
         ]
      <> [ testCase "bounded flush timeout recovers when the delayed response arrives" $
             withMockProducer $ \mock producer -> do
               calls <- newIORef 0
               reports <- newEmptyMVar
               delayMockProduce mock 300
               (result, weak) <- enqueueCapture producer mockRecord calls (Just reports)
               result @?= Right ()
               awaitCondition ((== 1) <$> mockReceivedProduce mock)
               flushProducerTimeout producer (Timeout 0) >>= (@?= Just (KafkaResponseError RdKafkaRespErrTimedOut))
               assertRetained weak
               flushProducerTimeout producer (Timeout 5000) >>= (@?= Nothing)
               takeWithin reports >>= (@?= DeliverySuccess mockRecord (C.Offset 0))
               awaitCollected weak
               readIORef calls >>= (@?= 1)
               flushProducerTimeout producer (Timeout 0) >>= (@?= Nothing)
               closeProducer producer
               closeProducer producer,
           testCase "close retries a timed-out flush until the delayed delivery completes" $
             withMockProducer $ \mock producer -> do
               calls <- newIORef 0
               reports <- newEmptyMVar
               delayMockProduce mock 1600
               (result, weak) <- enqueueCapture producer mockRecord calls (Just reports)
               result @?= Right ()
               awaitCondition ((== 1) <$> mockReceivedProduce mock)
               closeProducer producer
               takeWithin reports >>= (@?= DeliverySuccess mockRecord (C.Offset 0))
               awaitCollected weak
               readIORef calls >>= (@?= 1)
               flushProducerTimeout producer (Timeout 0) >>= (@?= Nothing),
           testCase "oversized rejection releases capture without a delivery report" $
             withProducer (extraProp "message.max.bytes" "1000") $ \producer -> do
               calls <- newIORef 0
               let oversized = record {prValue = Just (BS.replicate 2000 42)}
               (result, weak) <- enqueueCapture producer oversized calls Nothing
               result @?= Left (ImmediateError (KafkaResponseError RdKafkaRespErrMsgSizeTooLarge))
               awaitCollected weak
               flushProducerTimeout producer (Timeout 0) >>= (@?= Nothing)
               readIORef calls >>= (@?= 0),
           testCase "known invalid partition rejection releases capture" $
             withMockProducer $ \_ producer -> do
               calls <- newIORef 0
               let invalid = mockRecord {prPartition = SpecifiedPartition 2}
               (result, weak) <- enqueueCapture producer invalid calls Nothing
               result @?= Left (ImmediateError (KafkaResponseError RdKafkaRespErrUnknownPartition))
               awaitCollected weak
               produceMessage producer invalid >>= (@?= Just (KafkaResponseError RdKafkaRespErrUnknownPartition))
               flushProducerTimeout producer (Timeout 0) >>= (@?= Nothing)
               readIORef calls >>= (@?= 0),
           testCase "empty queue supports repeated flush and close" $
             withProducer mempty $ \producer -> do
               flushProducerTimeout producer (Timeout 0) >>= (@?= Nothing)
               purgeProducer producer [] >>= (@?= Nothing)
               closeProducer producer
               flushProducer producer
               closeProducer producer
         ]
      <> [ testCase ("rejects before native enqueue: " <> label) $
             withProducer mempty $ \producer -> withProducerTracking 0 $ do
               calls <- newIORef 0
               (result, weak) <- enqueueCapture producer invalid calls Nothing
               result @?= Left (ImmediateError (KafkaBadSpecification reason))
               awaitCollected weak
               nativeProducerProperty 1 >>= (@?= Nothing)
               flushProducerTimeout producer (Timeout 0) >>= (@?= Nothing)
               readIORef calls >>= (@?= 0)
         | (label, invalid, reason) <-
             [ ("NUL topic name", record {prTopic = TopicName (prefixedName env "producer-nul\0tail")}, "topic name contains NUL"),
               ("NUL header name", record {prHeaders = headersFromList [("name\0tail", "value")]}, "header name contains NUL"),
               ("maximum unrepresentable partition", record {prPartition = SpecifiedPartition maxBound}, "partition id is outside the C int range"),
               ("minimum unrepresentable partition", record {prPartition = SpecifiedPartition minBound}, "partition id is outside the C int range")
             ]
         ]
      <> [ testCase ("configuration rejection: " <> label) $ do
             result <- try @KafkaError (newProducer (brokersList [BrokerAddress "127.0.0.1:1"] <> props))
             case result of
               Left (KafkaUnknownConfigurationKey message) | unknown -> assertBool (Text.unpack message) (key `Text.isInfixOf` message)
               Left (KafkaInvalidConfigurationValue message) | not unknown -> assertBool (Text.unpack message) (key `Text.isInfixOf` message)
               Left exception -> assertFailure ("unexpected configuration exception: " <> show exception)
               Right (Left err) -> assertFailure ("configuration returned rather than throwing: " <> show err)
               Right (Right producer) -> releaseProducer producer >> assertFailure "invalid configuration was accepted"
         | (label, props, key, unknown) <-
             [ ("unknown global key", extraProp "not.a.producer.setting" "value", "not.a.producer.setting", True),
               ("unknown topic key", extraTopicProps (Map.singleton "not.a.topic.setting" "value"), "not.a.topic.setting", True),
               ("invalid topic timeout", sendTimeout (Timeout (-1)), "message.timeout.ms", False),
               ("excessive topic timeout", sendTimeout (Timeout maxBound), "message.timeout.ms", False),
               ("global-only key in topic map", extraTopicProps (Map.singleton "queue.buffering.max.messages" "1"), "queue.buffering.max.messages", True)
             ]
         ]
      <> [ testCase "later delivery callback replaces the earlier callback" $ do
             first <- newIORef (0 :: Int)
             lastReports <- newEmptyMVar
             let props =
                   setCallback (deliveryCallback (\_ -> atomicModifyIORef' first (\n -> (n + 1, ()))))
                     <> setCallback (deliveryCallback (putMVar lastReports))
             withProducer props $ \producer -> do
               produceMessage producer record >>= (@?= Nothing)
               purgeProducer producer [PurgeQueue] >>= (@?= Nothing)
               flushProducerTimeout producer (Timeout 1000) >>= (@?= Nothing)
               takeWithin lastReports >>= (@?= DeliveryFailure record (KafkaResponseError RdKafkaRespErrPurgeQueue))
               readIORef first >>= (@?= 0)
         ]
      <> [ testCase "null native delivery frame reports the generic errno fallback without dereferencing a message" $ withProducerTracking 6 $ do
             reports <- newEmptyMVar
             bracket (newProducer (producerProps env <> setCallback (deliveryCallback (putMVar reports))) >>= either throwIO pure) releaseProducer $ \_ -> do
               triggerNullDelivery >>= (@?= fromEnum RdKafkaRespErrFail)
               takeWithin reports >>= (@?= NoMessageError (KafkaResponseError RdKafkaRespErrFail)),
           testCase "native delivery without an opaque Haskell callback preserves the complete record" $
             withTopic env "producer-no-opaque" 1 $ \topic _ -> withProducerTracking 6 $ do
               reports <- newEmptyMVar
               bracket (newProducer (producerProps env <> setCallback (deliveryCallback (putMVar reports))) >>= either throwIO pure) releaseProducer $ \producer -> do
                 enqueueWithoutOpaque topic
                 flushProducerTimeout producer (Timeout 10000) >>= (@?= Nothing)
                 let expected = ProducerRecord topic (SpecifiedPartition 0) (Just "native-key") (Just "native-without-opaque") mempty
                 takeWithin reports >>= (@?= DeliverySuccess expected (C.Offset 0)),
           testCase "delivery header acquisition failure preserves the outcome with empty headers" $
             withTopic env "producer-header-failure" 1 $ \topic _ -> withProducerTracking 7 $ do
               global <- newEmptyMVar
               individual <- newEmptyMVar
               bracket (newProducer (producerProps env <> setCallback (deliveryCallback (putMVar global))) >>= either throwIO pure) releaseProducer $ \producer -> do
                 let original = ProducerRecord topic (SpecifiedPartition 0) (Just "key") (Just "payload") (headersFromList [("present", "before-failure")])
                     expected = DeliverySuccess (original {prHeaders = mempty}) (C.Offset 0)
                 produceMessage' producer original (putMVar individual) >>= (@?= Right ())
                 flushProducerTimeout producer (Timeout 10000) >>= (@?= Nothing)
                 takeWithin global >>= (@?= expected)
                 takeWithin individual >>= (@?= expected)
         ]
  where
    record = ProducerRecord (TopicName (prefixedName env "producer-regression")) (SpecifiedPartition 0) Nothing (Just "callback payload") (headersFromList [("producer-test", "present"), ("producer-test", BS.pack [0, 255])])
    withProducer props = withProducerRaw (extraProp "message.timeout.ms" "60000" <> props)
    withProducerRaw props =
      bracket
        ( newProducer (brokersList [BrokerAddress "127.0.0.1:1"] <> extraProp "log_level" "0" <> props)
            >>= either (assertFailure . show) pure
        )
        releaseProducer
    checkTimeout props = withProducer props $ \producer -> do
      reports <- newEmptyMVar
      produceMessage' producer record (putMVar reports) >>= (@?= Right ())
      flushProducerTimeout producer (Timeout 5000) >>= (@?= Nothing)
      takeWithin reports >>= (@?= DeliveryFailure record (KafkaResponseError RdKafkaRespErrMsgTimedOut))
    mockRecord = record {prTopic = TopicName "producer-inflight"}
    withMockProducer action = withProducerMock $ \mock addresses ->
      bracket (newProducer (brokersList addresses <> extraProp "log_level" "0") >>= either throwIO pure) releaseProducer $ \producer -> do
        metadata <- topicMetadata producer (Timeout 5000) (prTopic mockRecord)
        map (\t -> (tmTopicName t, map pmPartitionId (tmPartitions t))) . kmTopics <$> metadata
          @?= Right [(prTopic mockRecord, [PartitionId 0])]
        action mock producer

purgeCases :: [(String, [PurgeFlag])]
purgeCases =
  [ ("none", []),
    ("queue", [PurgeQueue]),
    ("inflight", [PurgeInFlight]),
    ("nonblocking", [PurgeNonBlocking]),
    ("queue+inflight", [PurgeQueue, PurgeInFlight]),
    ("queue+nonblocking", [PurgeQueue, PurgeNonBlocking]),
    ("inflight+nonblocking", [PurgeInFlight, PurgeNonBlocking]),
    ("all", [PurgeQueue, PurgeInFlight, PurgeNonBlocking])
  ]

-- Keep the captured IORef out of the caller's stack. The external count and
-- completion MVar let tests wait for the forked callback without rooting it.
{-# NOINLINE enqueueCapture #-}
enqueueCapture :: KafkaProducer -> ProducerRecord -> IORef Int -> Maybe (MVar DeliveryReport) -> IO (Either ImmediateError (), Weak (IORef Int))
enqueueCapture producer record calls completed = do
  captured <- newIORef (42 :: Int)
  weak <- mkWeakIORef captured (pure ())
  result <- produceMessage' producer record $ \report -> do
    readIORef captured >>= (@?= 42)
    atomicModifyIORef' calls (\n -> (n + 1, ()))
    mapM_ (`putMVar` report) completed
  pure (result, weak)

takeWithin :: MVar a -> IO a
takeWithin var = timeout 5000000 (takeMVar var) >>= maybe (assertFailure "callback did not complete within five seconds") pure

-- Separate processes contain foreign-boundary failures and changes to the
-- runtime's uncaught-exception handler. Preserve child coverage for the
-- collector, using unique files instead of the parent's HPCTIXFILE.
callbackChild :: String -> IO ()
callbackChild mode = do
  executable <- getExecutablePath
  environment <- getEnvironment
  directory <- maybe "dist-callback-probes" id <$> lookupEnv "RDKAFKA_TEST_CHILD_HPC_DIR"
  createDirectoryIfMissing True directory
  nonce <- randomIO :: IO Word
  let output = directory <> "/producer-" <> mode <> "-" <> show nonce <> ".tix"
      process =
        (proc executable (["--producer-callback-probe", mode, "+RTS", "--read-tix-file=no"] <> (if mode == "allocation" then ["-N1", "-A64m"] else []) <> ["-RTS"]))
          { env = Just (("HPCTIXFILE", output) : filter ((/= "HPCTIXFILE") . fst) environment)
          }
  result <- timeout 15000000 (readCreateProcessWithExitCode process "")
  case result of
    Just (ExitSuccess, _, _) -> pure ()
    other -> assertFailure ("callback probe failed: " <> show other)

runCallbackProbe :: String -> IO ()
runCallbackProbe "allocation" =
  bracket (newProducer (brokersList [BrokerAddress "127.0.0.1:1"]) >>= either (assertFailure . show) pure) releaseProducer $ \producer ->
    withProducerTracking 8 $ do
      calls <- newIORef 0
      ready <- newEmptyMVar
      -- Each borrowed header is small, but the final native union array is
      -- larger than the 64 KiB limit. Force the list before limiting RTS.
      let headers = headersFromList (replicate 2048 ("name", "value"))
          record = ProducerRecord (TopicName "producer-allocation-probe") (SpecifiedPartition 0) Nothing (Just "payload") headers
      evaluate (length (headersToList headers)) >>= (@?= 2048)
      outcome <- try @SomeException (enqueueAllocationCapture producer record calls ready)
      case outcome of
        Left exception -> fromException exception @?= Just HeapOverflow
        Right result -> assertFailure ("allocation unexpectedly succeeded: " <> show result)
      enqueueBoundaryEntered >>= (@?= False)
      takeWithin ready >>= awaitCollected
      readIORef calls >>= (@?= 0)
      flushProducerTimeout producer (Timeout 0) >>= (@?= Nothing)
runCallbackProbe mode = do
  reported <- newEmptyMVar
  bracket getUncaughtExceptionHandler setUncaughtExceptionHandler $ \_ -> do
    setUncaughtExceptionHandler (void . tryPutMVar reported . show)
    let global _ = if mode == "global" then throwIO (userError "producer-global-probe") else pure ()
        props = brokersList [BrokerAddress "127.0.0.1:1"] <> setCallback (deliveryCallback global)
        record = ProducerRecord (TopicName "producer-exception-probe") (SpecifiedPartition 0) Nothing (Just "payload") mempty
    bracket (newProducer props >>= either (assertFailure . show) pure) releaseProducer $ \producer -> do
      done <- newEmptyMVar
      calls <- newIORef 0
      weak <- enqueueThrowingCapture producer record calls done (mode == "message")
      assertRetained weak
      purgeProducer producer [PurgeQueue] >>= (@?= Nothing)
      flushProducerTimeout producer (Timeout 1000) >>= (@?= Nothing)
      takeWithin done >>= (@?= DeliveryFailure record (KafkaResponseError RdKafkaRespErrPurgeQueue))
      exception <- takeWithin reported
      assertBool exception (("producer-" <> mode <> "-probe") `isInfixOf` exception)
      awaitCollected weak
      readIORef calls >>= (@?= 1)

{-# NOINLINE enqueueAllocationCapture #-}
enqueueAllocationCapture :: KafkaProducer -> ProducerRecord -> IORef Int -> MVar (Weak (IORef Int)) -> IO (Either ImmediateError ())
enqueueAllocationCapture producer record calls ready = do
  captured <- newIORef (42 :: Int)
  weak <- mkWeakIORef captured (pure ())
  putMVar ready weak
  withPinnedAllocationLimit $ produceMessage' producer record $ \_ -> do
    readIORef captured >>= (@?= 42)
    atomicModifyIORef' calls (\n -> (n + 1, ()))

{-# NOINLINE enqueueThrowingCapture #-}
enqueueThrowingCapture :: KafkaProducer -> ProducerRecord -> IORef Int -> MVar DeliveryReport -> Bool -> IO (Weak (IORef Int))
enqueueThrowingCapture producer record calls done shouldThrow = do
  captured <- newIORef (42 :: Int)
  weak <- mkWeakIORef captured (pure ())
  produceMessage'
    producer
    record
    ( \report -> do
        readIORef captured >>= (@?= 42)
        atomicModifyIORef' calls (\n -> (n + 1, ()))
        putMVar done report
        if shouldThrow then throwIO (userError "producer-message-probe") else pure ()
    )
    >>= (@?= Right ())
  pure weak

{-# NOINLINE enqueueObservedCapture #-}
enqueueObservedCapture :: KafkaProducer -> ProducerRecord -> IORef Int -> MVar (Weak (IORef Int)) -> MVar DeliveryReport -> IO (Either ImmediateError ())
enqueueObservedCapture producer record calls ready reports = do
  captured <- newIORef (42 :: Int)
  weak <- mkWeakIORef captured (pure ())
  putMVar ready weak
  produceMessage' producer record $ \report -> do
    readIORef captured >>= (@?= 42)
    atomicModifyIORef' calls (\n -> (n + 1, ()))
    putMVar reports report

{-# NOINLINE enqueueBlockedCapture #-}
enqueueBlockedCapture :: KafkaProducer -> ProducerRecord -> MVar () -> MVar () -> MVar () -> IO (Weak (IORef Int))
enqueueBlockedCapture producer record started resume done = do
  captured <- newIORef (42 :: Int)
  weak <- mkWeakIORef captured (pure ())
  produceMessage'
    producer
    record
    ( \_ -> do
        getMaskingState >>= (@?= Unmasked)
        putMVar started ()
        readMVar resume
        readIORef captured >>= (@?= 42)
        putMVar done ()
    )
    >>= (@?= Right ())
  pure weak

awaitCondition :: IO Bool -> IO ()
awaitCondition condition = timeout 5000000 loop >>= maybe (assertFailure "thread state was not observed") pure
  where
    loop = condition >>= \ready -> if ready then pure () else threadDelay 1000 >> loop

roundTrip :: TestEnv -> KafkaCompressionCodec -> IO ()
roundTrip testEnv codec =
  withTopic testEnv suffix 1 $ \topic _ ->
    bracket
      (newProducer (producerProps testEnv <> topicCompression codec) >>= either throwIO pure)
      releaseProducer
      $ \producer -> withProducerTracking 0 $ do
        let values = [Nothing, Just BS.empty, Just (BS.pack [0, 255, 1])]
            headers = headersFromList [("", ""), ("duplicate", ""), ("duplicate", BS.pack [0, 255])]
            records = [ProducerRecord topic partition key value headers | (n, (key, value)) <- zip [0 :: Int ..] [(k, v) | k <- values, v <- values], let partition = if even n then SpecifiedPartition 0 else UnassignedPartition]
        sendMessages records producer >>= either throwIO pure
        nativeProducerProperty 2 >>= (@?= Just (Text.unpack (kafkaCompressionCodecToText codec)))
        let props = consumerProps testEnv <> C.groupId (makeGroupId testEnv suffix) <> C.callbackPollMode C.CallbackPollModeSync
        bracket (C.newAssignedConsumer props >>= either throwIO pure) releaseConsumer $ \consumer -> do
          C.assign consumer [C.TopicPartition topic (PartitionId 0) C.PartitionOffsetBeginning] >>= (@?= Nothing)
          actual <- receiveMessages consumer (length records) >>= either throwIO pure
          let identities = [ProducerRecord (C.crTopic r) (SpecifiedPartition 0) (C.crKey r) (C.crValue r) (C.crHeaders r) | r <- actual]
          identities @?= [r {prPartition = SpecifiedPartition 0} | r <- records]
          map C.crPartition actual @?= replicate (length records) (PartitionId 0)
          map C.crOffset actual @?= map C.Offset [0 .. fromIntegral (length records) - 1]
  where
    suffix = "producer-bytes-" <> kafkaCompressionCodecToText codec
