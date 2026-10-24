module Kafka.ConsumerStateSupport (withStateFault, endStateFault, stateStat, triggerRebalance, stateLeakControl, commitObservation, withQueueTracking, queueEventStat, awaitQueuedRecord) where

import Control.Concurrent (threadDelay)
import Control.Exception (bracket_)
import Data.Text qualified as Text
import Foreign.C.String (CString, withCString)
import Foreign.C.Types (CInt (..))
import Kafka.Types (TopicName (..))
import System.Timeout (timeout)
import Test.Tasty.HUnit (assertFailure)

foreign import ccall unsafe "rdkafka_hs_test_state_begin" begin :: CInt -> IO ()

foreign import ccall unsafe "rdkafka_hs_test_state_end" endStateFault :: IO ()

foreign import ccall unsafe "rdkafka_hs_test_state_stat" stat :: CInt -> IO CInt

foreign import ccall safe "rdkafka_hs_test_state_trigger" trigger :: CString -> IO ()

foreign import ccall unsafe "rdkafka_hs_test_state_leak_control" stateLeakControl :: IO ()

withStateFault :: Int -> IO a -> IO a
withStateFault mode = bracket_ (begin (fromIntegral mode)) endStateFault

stateStat :: Int -> IO Int
stateStat index = fromIntegral <$> stat (fromIntegral index)

triggerRebalance :: TopicName -> IO ()
triggerRebalance (TopicName topic) = withCString (Text.unpack topic) trigger

foreign import ccall unsafe "rdkafka_hs_test_state_commit_count" commitCount :: IO CInt

foreign import ccall unsafe "rdkafka_hs_test_state_commit_async" commitAsync :: IO CInt

commitObservation :: IO (Int, Int)
commitObservation = (,) <$> (fromIntegral <$> commitCount) <*> (fromIntegral <$> commitAsync)

foreign import ccall unsafe "rdkafka_hs_test_queue_begin" beginQueue :: IO ()

foreign import ccall unsafe "rdkafka_hs_test_queue_end" endQueue :: IO ()

foreign import ccall unsafe "rdkafka_hs_test_queue_stat" statQueue :: CInt -> IO CInt

withQueueTracking :: IO a -> IO a
withQueueTracking = bracket_ beginQueue endQueue

queueEventStat :: Int -> IO Int
queueEventStat index = fromIntegral <$> statQueue (fromIntegral index)

awaitQueuedRecord :: IO () -> IO ()
awaitQueuedRecord refresh = timeout 10000000 loop >>= maybe (assertFailure "record was not queued before enabling notification") pure
  where
    loop =
      queueEventStat 1 >>= \count ->
        if count < 0
          then assertFailure "notification destination queue was not observed"
          else if count > 0 then pure () else threadDelay 1000 >> loop
