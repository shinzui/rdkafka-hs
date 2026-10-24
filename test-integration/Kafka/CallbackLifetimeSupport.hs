module Kafka.CallbackLifetimeSupport (withCallbackTracking, awaitCallbackStage, releaseCallbackStage, callbackStat) where

import Control.Concurrent (threadDelay)
import Control.Exception (bracket_)
import Foreign.C.Types (CInt (..))
import System.Timeout (timeout)
import Test.Tasty.HUnit (assertFailure)

foreign import ccall unsafe "rdkafka_hs_test_callbacks_begin" begin :: CInt -> IO ()

foreign import ccall unsafe "rdkafka_hs_test_callbacks_end" end :: IO ()

foreign import ccall unsafe "rdkafka_hs_test_callbacks_release" releaseCallbackStage :: IO ()

foreign import ccall unsafe "rdkafka_hs_test_callbacks_stat" stat :: CInt -> IO CInt

withCallbackTracking :: Int -> IO a -> IO a
withCallbackTracking mode = bracket_ (begin (fromIntegral mode)) end

callbackStat :: Int -> IO Int
callbackStat index = fromIntegral <$> stat (fromIntegral index)

awaitCallbackStage :: IO ()
awaitCallbackStage = timeout 5000000 loop >>= maybe (assertFailure "native callback stage was not reached") pure
  where
    loop = callbackStat 0 >>= \n -> if n > 0 then pure () else threadDelay 1000 >> loop
