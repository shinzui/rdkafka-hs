-- | The queue event notification (IR-1) on an assign-only observer consumer
-- (IR-2), followed by teardown: the combined acceptance of
-- [consumer queue and teardown ownership](docs/adr/consumer-queue-and-teardown-ownership.md).
module Kafka.Integration.ObserverTest
  ( tests,
  )
where

import Control.Concurrent (threadWaitRead)
import Control.Exception (finally, onException, try)
import Control.Monad (void)
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.Text qualified as Text
import Foreign.Marshal.Alloc (allocaBytes)
import Foreign.Ptr (castPtr)
import GHC.Clock (getMonotonicTime)
import Kafka.Consumer
import Kafka.TestEnv
import System.Posix.IO (FdOption (..), closeFd, createPipe, fdReadBuf, setFdOption)
import System.Posix.Types (ByteCount, Fd)
import System.Timeout (timeout)
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (Assertion, assertFailure, testCase, (@?=))

tests :: TestEnv -> TestTree
tests env =
  testGroup
    "observer"
    [ testCase "async" (scenario env CallbackPollModeAsync "async"),
      testCase "sync" (scenario env CallbackPollModeSync "sync")
    ]

scenario :: TestEnv -> CallbackPollMode -> String -> Assertion
scenario env mode label =
  withTopic env (Text.pack ("observer-" <> label)) 1 $ \topic producer -> do
    produceTo producer topic 0 "before"
    let props =
          consumerProps env
            <> groupId (makeGroupId env (Text.pack ("observer-" <> label)))
            <> noAutoCommit
            <> callbackPollMode mode
    consumer <- newAssignedConsumer props >>= either (assertFailure . ("newAssignedConsumer: " <>) . show) pure
    (rfd, wfd) <- createPipe
    -- On failure: disable the event, destroy the consumer, then close the
    -- descriptors. On success the scenario does the first two itself.
    let cleanup = setConsumerQueueIOEvent consumer Nothing >> destroyConsumer consumer
    flip finally (closeFd rfd >> closeFd wfd) . (`onException` cleanup) $ do
      setFdOption rfd NonBlockingRead True
      setFdOption wfd NonBlockingRead True

      -- Start at the end of the partition: the observer is idle.
      assign consumer [TopicPartition topic (PartitionId 0) (PartitionOffset 1)] >>= (@?= Nothing)
      setConsumerQueueIOEvent consumer (Just (wfd, "1"))
      void (drainQueue consumer)
      void (drainPipe rfd)

      produceTo producer topic 0 "first"
      timeout 10000000 (threadWaitRead rfd) >>= (@?= Just ())
      void (drainPipe rfd)
      awaitAttributed consumer >>= (@?= (PartitionId 0, Offset 1, "first"))

      -- Reassigning (back to the start) keeps the notification flowing.
      void (drainQueue consumer)
      void (drainPipe rfd)
      assign consumer [TopicPartition topic (PartitionId 0) (PartitionOffset 0)] >>= (@?= Nothing)
      timeout 10000000 (threadWaitRead rfd) >>= (@?= Just ())
      void (drainPipe rfd)
      awaitAttributed consumer >>= (@?= (PartitionId 0, Offset 0, "before"))

      setConsumerQueueIOEvent consumer Nothing
      destroyConsumer consumer

-- | The next record's origin and value via 'pollMessageAttributed', skipping
-- timeouts and non-record events, for at most ten seconds.
awaitAttributed :: KafkaConsumer -> IO (PartitionId, Offset, ByteString)
awaitAttributed consumer = do
  deadline <- (+ 10) <$> getMonotonicTime
  let go = do
        now <- getMonotonicTime
        if now > deadline
          then assertFailure "no record arrived within 10 seconds"
          else
            pollMessageAttributed consumer (Timeout 500) >>= \case
              Right record -> pure (crPartition record, crOffset record, maybe "" id (crValue record))
              Left _ -> go
  go

drainQueue :: KafkaConsumer -> IO Int
drainQueue consumer = go (0 :: Int) (50 :: Int)
  where
    go n 0 = pure n
    go n budget =
      pollMessage consumer (Timeout 200) >>= \case
        Right _ -> go (n + 1) (budget - 1)
        Left (KafkaResponseError RdKafkaRespErrTimedOut) -> pure n
        Left _ -> go n (budget - 1)

drainPipe :: Fd -> IO ByteString
drainPipe fd =
  allocaBytes 64 $ \buf ->
    (try (fdReadBuf fd buf 64) :: IO (Either IOError ByteCount)) >>= \case
      Right count | count > 0 -> (<>) <$> BS.packCStringLen (castPtr buf, fromIntegral count) <*> drainPipe fd
      _ -> pure BS.empty
