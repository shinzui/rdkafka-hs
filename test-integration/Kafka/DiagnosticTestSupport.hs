module Kafka.DiagnosticTestSupport (withDiagnosticFault, diagnosticStat, diagnosticLeakControl, awaitDiagnosticBoundary, releaseDiagnosticBoundary) where

import Control.Concurrent (threadDelay)
import Control.Exception (bracket_)
import Foreign.C.Types (CInt (..))
import System.Timeout (timeout)
import Test.Tasty.HUnit (assertFailure)

foreign import ccall unsafe "rdkafka_hs_test_diagnostic_leak_control" diagnosticLeakControl :: IO ()

foreign import ccall unsafe "rdkafka_hs_test_diagnostic_begin" begin :: CInt -> IO ()

foreign import ccall unsafe "rdkafka_hs_test_diagnostic_end" end :: IO ()

foreign import ccall unsafe "rdkafka_hs_test_diagnostic_stat" stat :: CInt -> IO CInt

foreign import ccall unsafe "rdkafka_hs_test_diagnostic_entered" entered :: IO CInt

foreign import ccall unsafe "rdkafka_hs_test_diagnostic_release" releaseDiagnosticBoundary :: IO ()

withDiagnosticFault :: Int -> IO a -> IO a
withDiagnosticFault mode = bracket_ (begin (fromIntegral mode)) end

diagnosticStat :: Int -> IO Int
diagnosticStat index = fromIntegral <$> stat (fromIntegral index)

awaitDiagnosticBoundary :: IO ()
awaitDiagnosticBoundary = timeout 5000000 loop >>= maybe (assertFailure "native dump boundary was not reached") pure
  where
    loop = entered >>= \n -> if n == 1 then pure () else threadDelay 1000 >> loop
