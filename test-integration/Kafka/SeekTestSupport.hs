module Kafka.SeekTestSupport (withSeekFault, seekStat, awaitCommittedBoundary, releaseCommittedBoundary) where

import Control.Concurrent (threadDelay)
import Control.Exception (bracket_)
import Foreign.C.Types (CInt (..))
import System.Timeout (timeout)
import Test.Tasty.HUnit (assertFailure)

foreign import ccall unsafe "rdkafka_hs_test_seek_begin" begin :: CInt -> IO ()

foreign import ccall unsafe "rdkafka_hs_test_seek_end" end :: IO ()

foreign import ccall unsafe "rdkafka_hs_test_seek_release" releaseCommittedBoundary :: IO ()

foreign import ccall unsafe "rdkafka_hs_test_seek_stat" stat :: CInt -> IO CInt

withSeekFault :: Int -> IO a -> IO a
withSeekFault mode = bracket_ (begin (fromIntegral mode)) end

seekStat :: Int -> IO Int
seekStat index = fromIntegral <$> stat (fromIntegral index)

awaitCommittedBoundary :: IO ()
awaitCommittedBoundary = timeout 5000000 loop >>= maybe (assertFailure "committed lookup barrier not reached") pure
  where
    loop = seekStat 0 >>= \n -> if n == 1 then pure () else threadDelay 1000 >> loop
