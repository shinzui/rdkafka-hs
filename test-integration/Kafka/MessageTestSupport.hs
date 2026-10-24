module Kafka.MessageTestSupport (withMessageFault, messageStat, awaitMessageBoundary, releaseMessageBoundary) where

import Control.Concurrent (threadDelay)
import Control.Exception (bracket_)
import Foreign.C.Types (CInt (..))
import System.Timeout (timeout)
import Test.Tasty.HUnit (assertFailure)

foreign import ccall unsafe "rdkafka_hs_test_messages_begin" begin :: CInt -> IO ()

foreign import ccall safe "rdkafka_hs_test_messages_end" end :: IO ()

foreign import ccall unsafe "rdkafka_hs_test_messages_release" releaseMessageBoundary :: IO ()

foreign import ccall unsafe "rdkafka_hs_test_messages_stat" stat :: CInt -> IO CInt

withMessageFault :: Int -> IO a -> IO a
withMessageFault mode = bracket_ (begin (fromIntegral mode)) end

messageStat :: Int -> IO Int
messageStat index = fromIntegral <$> stat (fromIntegral index)

awaitMessageBoundary :: IO ()
awaitMessageBoundary = timeout 5000000 loop >>= maybe (assertFailure "message ownership boundary not reached") pure
  where
    loop = messageStat 4 >>= \value -> if value > 0 then pure () else threadDelay 1000 >> loop
