-- | Pipe notifications for the actual queue read by the public polling APIs.
module Kafka.Integration.QueueEventTest (tests) where

import Control.Concurrent (threadWaitRead)
import Control.Exception (bracket, throwIO, try)
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.Text qualified as Text
import Foreign.C.Error (Errno (..), eAGAIN, eBADF, eWOULDBLOCK)
import Foreign.Marshal.Alloc (allocaBytes)
import Foreign.Ptr (castPtr)
import GHC.Clock (getMonotonicTimeNSec)
import GHC.IO.Exception (ioe_errno)
import Kafka.Consumer
import Kafka.ConsumerStateSupport (awaitQueuedRecord, queueEventStat, withQueueTracking)
import Kafka.FixtureSupport (collectExactly)
import Kafka.MessageTestSupport (messageStat, withMessageFault)
import Kafka.Producer (KafkaProducer, ProducePartition (..), ProducerRecord (..))
import Kafka.TestEnv
import System.Posix.IO (FdOption (..), closeFd, createPipe, fdReadBuf, fdWrite, setFdOption)
import System.Posix.Types (ByteCount, Fd (..))
import System.Timeout (timeout)
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (Assertion, assertFailure, testCase, (@?=))

tests :: TestEnv -> TestTree
tests env =
  testGroup
    "queue event"
    [ testCase "empty pipes are idle but invalid descriptor errors are preserved" $
        bracket createPipe closePipe $ \(rfd, _) -> do
          setFdOption rfd NonBlockingRead True
          fdRead rfd 1 >>= (@?= BS.empty)
          try @IOError (fdRead (Fd (-1)) 1) >>= \case
            Left err -> let Errno code = eBADF in ioe_errno err @?= Just code
            Right bytes -> assertFailure ("invalid descriptor appeared idle: " <> show bytes),
      modeGroup env "async" CallbackPollModeAsync,
      modeGroup env "sync" CallbackPollModeSync
    ]

modeGroup :: TestEnv -> String -> CallbackPollMode -> TestTree
modeGroup env label mode =
  testGroup
    label
    [ testCase "wakes an idle consumer and stays quiet once disabled" $
        withQueueEvent env mode (label <> "-idle") $ \consumer producer topic pipe@(rfd, wfd) -> do
          register consumer topic pipe
          send producer topic "first"
          timeout 10000000 (threadWaitRead rfd) >>= (@?= Just ())
          fdRead rfd 1 >>= (@?= "1")
          awaitRecord consumer topic 0 "first"
          drainQueue consumer topic >>= (@?= [])
          setConsumerQueueIOEvent consumer Nothing
          _ <- drainPipe rfd
          send producer topic "second"
          timeout 2000000 (threadWaitRead rfd) >>= (@?= Nothing)
          awaitRecord consumer topic 1 "second",
      testCase "finds records queued before it was enabled, then rearms" $
        withQueueEvent env mode (label <> "-preloaded") $ \consumer producer topic (rfd, wfd) -> do
          send producer topic "early"
          -- Observe a real queued record, rather than infer prefetch from a delay.
          awaitQueuedRecord (setConsumerQueueIOEvent consumer Nothing)
          setConsumerQueueIOEvent consumer (Just (wfd, "1"))
          drainQueue consumer topic >>= (@?= [(Offset 0, "early")])
          _ <- drainPipe rfd
          send producer topic "later"
          timeout 10000000 (threadWaitRead rfd) >>= (@?= Just ())
          awaitRecord consumer topic 1 "later",
      testCase "one wake-up covers several records, then rearms" $
        withQueueEvent env mode (label <> "-multiple") $ \consumer producer topic pipe@(rfd, _) -> do
          register consumer topic pipe
          sendAll producer topic ["one", "two", "three"]
          timeout 10000000 (threadWaitRead rfd) >>= (@?= Just ())
          drainPipe rfd >>= (@?= "1")
          collect consumer topic 3 >>= (@?= [(Offset 0, "one"), (Offset 1, "two"), (Offset 2, "three")])
          drainQueue consumer topic >>= (@?= [])
          _ <- drainPipe rfd
          send producer topic "four"
          timeout 10000000 (threadWaitRead rfd) >>= (@?= Just ())
          awaitRecord consumer topic 3 "four",
      testCase "binary payloads retain every byte and empty payloads preserve records" $
        withQueueEvent env mode (label <> "-payload") $ \consumer producer topic (rfd, wfd) -> do
          let payload = BS.pack [0, 255, 0, 65, 66, 67, 0, 128]
          setConsumerQueueIOEvent consumer (Just (wfd, payload))
          drainQueue consumer topic >>= (@?= [])
          _ <- drainPipe rfd
          send producer topic "binary-payload"
          timeout 10000000 (threadWaitRead rfd) >>= (@?= Just ())
          fdRead rfd (BS.length payload) >>= (@?= payload)
          awaitRecord consumer topic 0 "binary-payload"
          drainPipe rfd >>= (@?= BS.empty)
          setConsumerQueueIOEvent consumer (Just (wfd, BS.empty))
          send producer topic "empty-payload"
          awaitQueuedRecord (setConsumerQueueIOEvent consumer (Just (wfd, BS.empty)))
          timeout 500000 (threadWaitRead rfd) >>= (@?= Nothing)
          awaitRecord consumer topic 1 "empty-payload"
          drainPipe rfd >>= (@?= BS.empty),
      testCase "native record errors are never treated as idle" $
        withQueueEvent env mode (label <> "-error") $ \consumer producer topic _ -> do
          send producer topic "error-record"
          awaitQueuedRecord (setConsumerQueueIOEvent consumer Nothing)
          withMessageFault (if mode == CallbackPollModeAsync then 12 else 15) $ do
            result <- try @KafkaError (drainQueue consumer topic)
            result @?= Left (KafkaResponseError RdKafkaRespErrBadMsg)
            mapM messageStat [1, 2] >>= (@?= [1, 1]),
      testCase "close stops notifications and keeps caller descriptors open" $
        withQueueEvent env mode (label <> "-close") $ \consumer _ topic pipe@(rfd, wfd) -> do
          register consumer topic pipe
          closeConsumer consumer >>= (@?= Nothing)
          consumerClosed consumer >>= (@?= True)
          calls <- queueEventStat 0
          setConsumerQueueIOEvent consumer (Just (wfd, "1"))
          queueEventStat 0 >>= (@?= calls)
          _ <- drainPipe rfd
          timeout 500000 (threadWaitRead rfd) >>= (@?= Nothing)
          assertPipeOwned pipe,
      testCase "destroy stops notifications and keeps caller descriptors open" $
        withQueueEvent env mode (label <> "-destroy") $ \consumer _ topic pipe@(rfd, _) -> do
          register consumer topic pipe
          destroyConsumer consumer
          _ <- drainPipe rfd
          timeout 500000 (threadWaitRead rfd) >>= (@?= Nothing)
          assertPipeOwned pipe
    ]

register :: KafkaConsumer -> TopicName -> (Fd, Fd) -> IO ()
register consumer topic (rfd, wfd) = do
  setConsumerQueueIOEvent consumer (Just (wfd, "1"))
  drainQueue consumer topic >>= (@?= [])
  _ <- drainPipe rfd
  pure ()

-- Descriptors outlive consumer destruction, including failing test actions.
withQueueEvent :: TestEnv -> CallbackPollMode -> String -> (KafkaConsumer -> KafkaProducer -> TopicName -> (Fd, Fd) -> Assertion) -> Assertion
withQueueEvent env mode label body = withQueueTracking $
  withTopic env ("queue-event-" <> Text.pack label) 1 $ \topic producer ->
    bracket createPipe closePipe $ \pipe@(rfd, wfd) -> do
      setFdOption wfd NonBlockingRead True
      setFdOption rfd NonBlockingRead True
      let props = consumerProps env <> groupId (makeGroupId env ("queue-event-" <> Text.pack label)) <> callbackPollMode mode
      bracket (mkConsumerFor topic props) releaseConsumer $ \consumer -> do
        -- The observer borrows this final destination queue only while open.
        setConsumerQueueIOEvent consumer Nothing
        drainQueue consumer topic >>= (@?= [])
        body consumer producer topic pipe

closePipe :: (Fd, Fd) -> IO ()
closePipe (rfd, wfd) = closeFd rfd >> closeFd wfd

assertPipeOwned :: (Fd, Fd) -> Assertion
assertPipeOwned (rfd, wfd) = do
  fdWrite wfd "owner" >>= (@?= 5)
  fdRead rfd 5 >>= (@?= "owner")

send :: KafkaProducer -> TopicName -> ByteString -> IO ()
send producer topic value = sendAll producer topic [value]

sendAll :: KafkaProducer -> TopicName -> [ByteString] -> IO ()
sendAll producer topic values =
  sendMessages [ProducerRecord topic (SpecifiedPartition 0) Nothing (Just value) mempty | value <- values] producer >>= either throwIO pure

recordValue :: TopicName -> ConsumerRecord (Maybe ByteString) (Maybe ByteString) -> IO (Offset, ByteString)
recordValue topic record = do
  (crTopic record, crPartition record, crKey record, crHeaders record) @?= (topic, PartitionId 0, Nothing, mempty)
  value <- maybe (assertFailure "unexpected tombstone") pure (crValue record)
  pure (crOffset record, value)

drainQueue :: KafkaConsumer -> TopicName -> IO [(Offset, ByteString)]
drainQueue consumer topic = go (50 :: Int)
  where
    go 0 = assertFailure "queue did not become idle within fifty polls"
    go n =
      pollMessage consumer (Timeout 200) >>= \case
        Right record -> do
          value <- recordValue topic record
          (value :) <$> go (n - 1)
        Left (KafkaResponseError RdKafkaRespErrTimedOut) -> pure []
        Left (KafkaResponseError RdKafkaRespErrPartitionEof) -> go (n - 1)
        Left err -> throwIO err

fdRead :: Fd -> Int -> IO ByteString
fdRead fd n = allocaBytes n $ \buf ->
  (try (fdReadBuf fd buf (fromIntegral n)) :: IO (Either IOError ByteCount)) >>= \case
    Right count -> BS.packCStringLen (castPtr buf, fromIntegral count)
    Left err | maybe False ((`elem` [eAGAIN, eWOULDBLOCK]) . Errno) (ioe_errno err) -> pure BS.empty
    Left err -> throwIO err

drainPipe :: Fd -> IO ByteString
drainPipe rfd =
  fdRead rfd 64 >>= \case
    bytes | not (BS.null bytes) -> (bytes <>) <$> drainPipe rfd
    _ -> pure BS.empty

awaitRecord :: KafkaConsumer -> TopicName -> Int -> ByteString -> Assertion
awaitRecord consumer topic offset value = collect consumer topic 1 >>= (@?= [(Offset (fromIntegral offset), value)])

collect :: KafkaConsumer -> TopicName -> Int -> IO [(Offset, ByteString)]
collect consumer topic n =
  collectExactly getMonotonicTimeNSec 10000000000 n (pollMessage consumer (Timeout 500))
    >>= either throwIO (traverse (recordValue topic))
