-- | 'setConsumerQueueIOEvent' wakes a waiting application through a pipe
-- (improvement request IR-1, AC-2), in both callback poll modes.
--
-- librdkafka writes the payload when an op lands in the empty queue, at most
-- once until the queue is next polled, so every case follows the documented
-- protocol: enable, poll the queue empty, discard pending payload bytes, then
-- wait.
module Kafka.Integration.QueueEventTest
  ( tests,
  )
where

import Control.Concurrent (threadDelay, threadWaitRead)
import Control.Exception (bracket, finally, try)
import Control.Monad (void)
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.Either (isRight)
import Data.Text qualified as Text
import Foreign.Marshal.Alloc (allocaBytes)
import Foreign.Ptr (castPtr)
import GHC.Clock (getMonotonicTime)
import Kafka.Consumer
import Kafka.Producer (KafkaProducer, ProducePartition (..), ProducerRecord (..), closeProducer, flushProducer, produceMessage)
import Kafka.TestEnv
import Kafka.Topic (NewTopic (..), PartitionCount (..), ReplicationFactor (..), createTopic, deleteTopic)
import System.Posix.IO (FdOption (..), closeFd, createPipe, fdReadBuf, setFdOption)
import System.Posix.Types (ByteCount, Fd)
import System.Timeout (timeout)
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (Assertion, assertBool, assertFailure, testCase, (@?=))

tests :: TestEnv -> TestTree
tests env =
  testGroup
    "queue event"
    [ modeGroup env "async" CallbackPollModeAsync,
      modeGroup env "sync" CallbackPollModeSync
    ]

modeGroup :: TestEnv -> String -> CallbackPollMode -> TestTree
modeGroup env label mode =
  testGroup
    label
    [ testCase "wakes an idle consumer and stays quiet once disabled" $
        withQueueEvent env mode (label <> "-idle") $ \consumer producer topic (rfd, wfd) -> do
          register consumer (rfd, wfd)
          send producer topic "first"
          readable <- timeout 10000000 (threadWaitRead rfd)
          readable @?= Just ()
          fdRead rfd 1 >>= (@?= "1")
          awaitRecord consumer "first"

          void (drainQueue consumer)
          setConsumerQueueIOEvent consumer Nothing
          void (drainPipe rfd)
          send producer topic "second"
          quiet <- timeout 2000000 (threadWaitRead rfd)
          quiet @?= Nothing
          awaitRecord consumer "second",
      testCase "finds records queued before it was enabled, then rearms" $
        withQueueEvent env mode (label <> "-preloaded") $ \consumer producer topic (rfd, wfd) -> do
          -- Let the consumer fetch a record before the event is enabled; the
          -- protocol's poll after enabling must find it without a wake-up.
          send producer topic "early"
          threadDelay 3000000
          setConsumerQueueIOEvent consumer (Just (wfd, "1"))
          early <- drainQueue consumer
          assertBool ("expected the queued record, got " <> show early) ("early" `elem` early)
          void (drainPipe rfd)

          send producer topic "later"
          readable <- timeout 10000000 (threadWaitRead rfd)
          readable @?= Just ()
          awaitRecord consumer "later",
      testCase "one wake-up covers several records, then rearms" $
        withQueueEvent env mode (label <> "-multiple") $ \consumer producer topic (rfd, wfd) -> do
          register consumer (rfd, wfd)
          sendAll producer topic ["one", "two", "three"]
          readable <- timeout 10000000 (threadWaitRead rfd)
          readable @?= Just ()
          drainPipe rfd >>= (@?= "1")
          collected <- collect consumer 3
          collected @?= ["one", "two", "three"]
          void (drainQueue consumer)
          void (drainPipe rfd)

          send producer topic "four"
          rearmed <- timeout 10000000 (threadWaitRead rfd)
          rearmed @?= Just ()
          awaitRecord consumer "four"
    ]

-- | The wait protocol's first step: enable the event, poll the queue empty and
-- discard pending payload bytes.
register :: KafkaConsumer -> (Fd, Fd) -> IO ()
register consumer (rfd, wfd) = do
  setConsumerQueueIOEvent consumer (Just (wfd, "1"))
  void (drainQueue consumer)
  void (drainPipe rfd)

-- | Create a topic, a consumer subscribed to it and a producer, and open a
-- pipe whose ends are non-blocking. Cleanup disables the event before
-- closing the pipe, then releases the clients and deletes the topic.
withQueueEvent ::
  TestEnv ->
  CallbackPollMode ->
  String ->
  (KafkaConsumer -> KafkaProducer -> TopicName -> (Fd, Fd) -> Assertion) ->
  Assertion
withQueueEvent env mode label body = do
  let topic = TopicName (prefixedName env ("queue-event-" <> Text.pack label))
      props =
        consumerProps env
          <> groupId (makeGroupId env ("queue-event-" <> Text.pack label))
          <> callbackPollMode mode
  producer <- mkProducer env
  flip finally (void (deleteTopic producer topic) >> closeProducer producer) $ do
    created <- createTopic producer (NewTopic topic (PartitionCount 1) (ReplicationFactor 1) mempty)
    assertBool ("createTopic: " <> show created) (isRight created)
    consumer <- mkConsumerFor topic props
    flip finally (void (closeConsumer consumer)) $
      bracket createPipe (closePipe consumer) $ \(rfd, wfd) -> do
        -- NonBlockingRead sets O_NONBLOCK, which governs writes too.
        setFdOption wfd NonBlockingRead True
        setFdOption rfd NonBlockingRead True
        void (drainQueue consumer)
        body consumer producer topic (rfd, wfd)
  where
    closePipe consumer (rfd, wfd) = do
      setConsumerQueueIOEvent consumer Nothing
      closeFd rfd
      closeFd wfd

send :: KafkaProducer -> TopicName -> ByteString -> IO ()
send producer topic value = sendAll producer topic [value]

sendAll :: KafkaProducer -> TopicName -> [ByteString] -> IO ()
sendAll producer topic values = do
  mapM_ (\v -> produceMessage producer (ProducerRecord topic (SpecifiedPartition 0) Nothing (Just v) mempty)) values
  flushProducer producer

-- | Poll until a poll times out, returning the values received. Other errors
-- (non-record events) are skipped.
drainQueue :: KafkaConsumer -> IO [ByteString]
drainQueue consumer = go (50 :: Int)
  where
    go 0 = pure []
    go n =
      pollMessage consumer (Timeout 200) >>= \case
        Right record -> maybe id (:) (crValue record) <$> go (n - 1)
        Left (KafkaResponseError RdKafkaRespErrTimedOut) -> pure []
        Left _ -> go (n - 1)

-- | Read up to @n@ bytes from a non-blocking descriptor; empty when none are
-- pending.
fdRead :: Fd -> Int -> IO ByteString
fdRead fd n =
  allocaBytes n $ \buf ->
    (try (fdReadBuf fd buf (fromIntegral n)) :: IO (Either IOError ByteCount)) >>= \case
      Right count -> BS.packCStringLen (castPtr buf, fromIntegral count)
      Left _ -> pure BS.empty

-- | Read and discard pending payload bytes without blocking.
drainPipe :: Fd -> IO ByteString
drainPipe rfd =
  fdRead rfd 64 >>= \case
    bytes | not (BS.null bytes) -> (bytes <>) <$> drainPipe rfd
    _ -> pure BS.empty

-- | Poll, tolerating non-record events, until the record arrives (ten seconds).
awaitRecord :: KafkaConsumer -> ByteString -> Assertion
awaitRecord consumer value = do
  got <- collect consumer 1
  got @?= [value]

-- | Poll until @n@ records have arrived or ten seconds pass.
collect :: KafkaConsumer -> Int -> IO [ByteString]
collect consumer n = do
  deadline <- (+ 10) <$> getMonotonicTime
  let go acc
        | length acc >= n = pure (reverse acc)
        | otherwise = do
            now <- getMonotonicTime
            if now > deadline
              then assertFailure ("received only " <> show (reverse acc) <> " within 10 seconds")
              else
                pollMessage consumer (Timeout 500) >>= \case
                  Right record -> go (maybe acc (: acc) (crValue record))
                  Left _ -> go acc
  go []
