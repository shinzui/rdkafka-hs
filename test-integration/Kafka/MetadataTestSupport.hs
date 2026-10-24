module Kafka.MetadataTestSupport (withMetadataFault, metadataStat, awaitMetadataBoundary, releaseMetadataBoundary, metadataLeakControl) where

import Control.Concurrent (threadDelay)
import Control.Exception (bracket_)
import Data.Text qualified as Text
import Foreign.C.String (CString, withCString)
import Foreign.C.Types (CInt (..))
import Kafka.Types (BrokerAddress (..))
import System.Timeout (timeout)
import Test.Tasty.HUnit (assertFailure)

foreign import ccall unsafe "rdkafka_hs_test_metadata_begin" begin :: CInt -> IO ()

foreign import ccall unsafe "rdkafka_hs_test_metadata_end" end :: IO ()

foreign import ccall unsafe "rdkafka_hs_test_metadata_stat" stat :: CInt -> IO CInt

foreign import ccall unsafe "rdkafka_hs_test_metadata_entered" entered :: IO CInt

foreign import ccall unsafe "rdkafka_hs_test_metadata_release" releaseMetadataBoundary :: IO ()

foreign import ccall safe "rdkafka_hs_test_metadata_control" control :: CString -> IO ()

withMetadataFault :: Int -> IO a -> IO a
withMetadataFault value = bracket_ (begin (fromIntegral value)) end

metadataStat :: Int -> IO Int
metadataStat index = fromIntegral <$> stat (fromIntegral index)

awaitMetadataBoundary :: IO ()
awaitMetadataBoundary = timeout 10000000 loop >>= maybe (assertFailure "metadata acquisition boundary was not reached") pure
  where
    loop = entered >>= \n -> if n == 1 then pure () else threadDelay 1000 >> loop

metadataLeakControl :: [BrokerAddress] -> IO ()
metadataLeakControl brokers = withCString (Text.unpack (Text.intercalate "," [address | BrokerAddress address <- brokers])) control
