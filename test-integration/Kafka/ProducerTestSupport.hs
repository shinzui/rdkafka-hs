module Kafka.ProducerTestSupport (withProducerTracking, withPinnedAllocationLimit, enqueueBoundaryEntered, awaitEnqueueBoundary, releaseEnqueueBoundary, nativeProducerProperty, triggerNullDelivery, enqueueWithoutOpaque, ProducerMock, withProducerMock, delayMockProduce, mockReceivedProduce) where

import Control.Concurrent (threadDelay)
import Control.Exception (bracket, bracket_)
import Data.Text qualified as T
import Foreign.C.String (CString, peekCString, withCString)
import Foreign.C.Types (CInt (..))
import Foreign.Ptr (Ptr, nullPtr)
import Kafka.TestEnv (resolveBrokers)
import Kafka.Types (BrokerAddress, TopicName (..))
import System.Timeout (timeout)
import Test.Tasty.HUnit (assertFailure, (@?=))

foreign import ccall unsafe "rdkafka_hs_test_producer_begin" begin :: CInt -> IO ()

foreign import ccall unsafe "rdkafka_hs_test_producer_end" end :: IO ()

foreign import ccall unsafe "rdkafka_hs_test_producer_entered" entered :: IO CInt

foreign import ccall unsafe "rdkafka_hs_test_producer_heap_limit_begin" heapLimitBegin :: IO ()

foreign import ccall unsafe "rdkafka_hs_test_producer_heap_limit_end" heapLimitEnd :: IO ()

-- Only call in the isolated single-capability allocation probe.
withPinnedAllocationLimit :: IO a -> IO a
withPinnedAllocationLimit = bracket_ heapLimitBegin heapLimitEnd

enqueueBoundaryEntered :: IO Bool
enqueueBoundaryEntered = (== 1) <$> entered

foreign import ccall unsafe "rdkafka_hs_test_producer_release" releaseEnqueueBoundary :: IO ()

foreign import ccall unsafe "rdkafka_hs_test_producer_property" property :: CInt -> IO CString

withProducerTracking :: Int -> IO a -> IO a
withProducerTracking mode = bracket_ (begin (fromIntegral mode)) end

awaitEnqueueBoundary :: IO ()
awaitEnqueueBoundary = timeout 5000000 loop >>= maybe (assertFailure "native enqueue boundary was not reached") pure
  where
    loop = entered >>= \n -> if n == 1 then pure () else threadDelay 1000 >> loop

nativeProducerProperty :: Int -> IO (Maybe String)
nativeProducerProperty index =
  property (fromIntegral index) >>= \ptr ->
    if ptr == nullPtr then pure Nothing else Just <$> peekCString ptr

foreign import ccall safe "rdkafka_hs_test_producer_null_delivery" nullDelivery :: IO CInt

foreign import ccall safe "rdkafka_hs_test_producer_without_opaque" withoutOpaque :: CString -> IO CInt

triggerNullDelivery :: IO Int
triggerNullDelivery = fromIntegral <$> nullDelivery

enqueueWithoutOpaque :: TopicName -> IO ()
enqueueWithoutOpaque (TopicName topic) = withCString (T.unpack topic) withoutOpaque >>= (@?= 0)

data ProducerMock

foreign import ccall safe "rdkafka_hs_test_producer_mock_new" newMock :: IO (Ptr ProducerMock)

foreign import ccall safe "rdkafka_hs_test_producer_mock_destroy" destroyMock :: Ptr ProducerMock -> IO ()

foreign import ccall unsafe "rdkafka_hs_test_producer_mock_brokers" mockBrokers :: Ptr ProducerMock -> IO CString

foreign import ccall safe "rdkafka_hs_test_producer_mock_delay" mockDelay :: Ptr ProducerMock -> CInt -> IO CInt

foreign import ccall safe "rdkafka_hs_test_producer_mock_received" received :: Ptr ProducerMock -> IO CInt

withProducerMock :: (Ptr ProducerMock -> [BrokerAddress] -> IO a) -> IO a
withProducerMock action = bracket acquire destroyMock $ \mock -> do
  addresses <- mockBrokers mock >>= peekCString >>= either assertFailure pure . resolveBrokers . Just
  action mock addresses
  where
    acquire = newMock >>= \ptr -> if ptr == nullPtr then assertFailure "native mock cluster creation failed" else pure ptr

delayMockProduce :: Ptr ProducerMock -> Int -> IO ()
delayMockProduce mock milliseconds = mockDelay mock (fromIntegral milliseconds) >>= (@?= 0)

mockReceivedProduce :: Ptr ProducerMock -> IO Int
mockReceivedProduce mock = fromIntegral <$> received mock
