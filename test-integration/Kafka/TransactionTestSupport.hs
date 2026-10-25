module Kafka.TransactionTestSupport (withTxFault, txStat, awaitTxBoundary, releaseTxBoundary, nativeLeakControl, requireNativeFlagDiagnostics) where

import Control.Concurrent (threadDelay)
import Control.Exception (bracket_)
import Data.Int (Int64)
import Foreign.C.Types (CInt (..))
import System.Timeout (timeout)
import Test.Tasty.HUnit (assertBool, assertFailure)

foreign import ccall unsafe "rdkafka_hs_test_tx_begin" begin :: CInt -> IO ()

foreign import ccall unsafe "rdkafka_hs_test_tx_end" end :: IO ()

foreign import ccall unsafe "rdkafka_hs_test_tx_release" releaseTxBoundary :: IO ()

foreign import ccall unsafe "rdkafka_hs_test_tx_stat" stat :: CInt -> IO Int64

foreign import ccall safe "rdkafka_hs_test_tx_leak_control" nativeLeakControl :: IO ()

foreign import ccall unsafe "rdkafka_hs_test_tx_flag_diagnostics_available" flagDiagnosticsAvailable :: IO CInt

requireNativeFlagDiagnostics :: IO ()
requireNativeFlagDiagnostics = flagDiagnosticsAvailable >>= assertBool "Native flag controls require the diagnostic librdkafka build exposing its three private error-flag setters; see docs/testing/coverage.md." . (/= 0)

withTxFault :: Int -> IO a -> IO a
withTxFault value = bracket_ (begin (fromIntegral value)) end

txStat :: Int -> IO Int64
txStat = stat . fromIntegral

awaitTxBoundary :: IO ()
awaitTxBoundary = timeout 5000000 loop >>= maybe (assertFailure "transaction decoder boundary not reached") pure
  where
    loop = txStat 9 >>= \n -> if n == 1 then pure () else threadDelay 1000 >> loop
