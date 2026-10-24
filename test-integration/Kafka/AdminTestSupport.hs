{-# LANGUAGE ForeignFunctionInterface #-}

-- | Test executable instrumentation of real native admin ownership. No
-- production module or public API exposes these controls.
module Kafka.AdminTestSupport
  ( NativeKind (..),
    withNativeTracking,
    assertBalanced,
    counts,
    peak,
    failConfiguration,
    blockConfiguration,
    configurationEntered,
    withLeakControl,
    submissions,
    configurationCalls,
    lastConfiguration,
    rejectConfigurationAt,
    ResponseMode (..),
    responseMode,
    timeoutFault,
    timeoutReads,
  )
where

import Control.Exception (bracket, bracket_)
import Data.IORef (newIORef, readIORef, writeIORef)
import Foreign.C.String (CString, peekCString)
import Foreign.C.Types (CInt (..))
import Foreign.Ptr (Ptr, nullPtr)
import Test.Tasty.HUnit (assertBool, (@?=))

foreign import ccall unsafe "rdkafka_hs_test_admin_timeout_fault" setTimeoutFault :: CInt -> IO ()

foreign import ccall unsafe "rdkafka_hs_test_admin_timeout_reads" nativeTimeoutReads :: IO CInt

timeoutFault :: Int -> IO ()
timeoutFault = setTimeoutFault . fromIntegral

timeoutReads :: IO Int
timeoutReads = fromIntegral <$> nativeTimeoutReads

data ResponseMode = NativeResponse | WrongEvent | EmptyResult | WrongTopic | MissingPoll | DelayedResponse
  deriving stock (Enum, Show)

foreign import ccall unsafe "rdkafka_hs_test_admin_configuration_calls" configurationCallCount :: IO CInt

foreign import ccall unsafe "rdkafka_hs_test_admin_response_mode" setResponseMode :: CInt -> IO ()

foreign import ccall unsafe "rdkafka_hs_test_admin_last_key" lastKey :: IO CString

foreign import ccall unsafe "rdkafka_hs_test_admin_last_value" lastValue :: IO CString

lastConfiguration :: IO (String, String)
lastConfiguration = (,) <$> (lastKey >>= peekCString) <*> (lastValue >>= peekCString)

configurationCalls :: IO Int
configurationCalls = fromIntegral <$> configurationCallCount

rejectConfigurationAt :: Int -> IO ()
rejectConfigurationAt = failConfig . fromIntegral

responseMode :: ResponseMode -> IO ()
responseMode = setResponseMode . fromIntegral . fromEnum

data NativeKind = NativeNewTopic | NativeDeleteTopic | NativeQueue | NativeOptions | NativeEvent
  deriving stock (Enum, Bounded, Show)

foreign import ccall unsafe "rdkafka_hs_test_admin_submissions" submissionCount :: IO CInt

submissions :: IO Int
submissions = fromIntegral <$> submissionCount

foreign import ccall unsafe "rdkafka_hs_test_admin_begin" begin :: IO CInt

foreign import ccall unsafe "rdkafka_hs_test_admin_end" end :: IO ()

foreign import ccall unsafe "rdkafka_hs_test_admin_count" count :: CInt -> CInt -> IO CInt

foreign import ccall unsafe "rdkafka_hs_test_admin_fail_config" failConfig :: CInt -> IO ()

foreign import ccall unsafe "rdkafka_hs_test_admin_block_config" blockConfig :: CInt -> IO ()

foreign import ccall unsafe "rdkafka_hs_test_admin_config_entered" configEntered :: IO CInt

foreign import ccall unsafe "rdkafka_hs_test_admin_reject_control" rejectControl :: Ptr () -> IO CInt

foreign import ccall unsafe "rdkafka_hs_test_admin_positive_control" allocateControl :: IO (Ptr ())

foreign import ccall unsafe "rdkafka_hs_test_admin_release_control" releaseControl :: Ptr () -> IO ()

withNativeTracking :: IO a -> IO a
withNativeTracking = bracket_ (begin >>= (@?= 0)) end

counts :: NativeKind -> IO (Int, Int)
counts kind = do
  acquired <- count (fromIntegral (fromEnum kind)) 0
  released <- count (fromIntegral (fromEnum kind)) 1
  pure (fromIntegral acquired, fromIntegral released)

peak :: NativeKind -> IO Int
peak kind = fromIntegral <$> count (fromIntegral (fromEnum kind)) 3

assertBalanced :: IO ()
assertBalanced = mapM_ check [minBound .. maxBound]
  where
    check kind = do
      (allocated, released) <- counts kind
      assertBool (show kind <> " allocation/release imbalance: " <> show (allocated, released)) (allocated == released)

failConfiguration :: Bool -> IO ()
failConfiguration flag = failConfig (if flag then 1 else 0)

blockConfiguration :: Bool -> IO ()
blockConfiguration flag = blockConfig (if flag then 1 else 0)

configurationEntered :: IO Bool
configurationEntered = (/= 0) <$> configEntered

withLeakControl :: IO a -> IO a
withLeakControl action = do
  held <- newIORef Nothing
  let cleanup = failConfig 0 >> (readIORef held >>= mapM_ releaseControl)
      -- Reconstruct the inherited pattern: acquisition allocates a request,
      -- configuration rejects it, then acquisition returns Left. The outer
      -- bracket sees no handle to release. A separate control root allows the
      -- test to reclaim it after observing the missing release.
      oldAcquire = do
        pointer <- allocateControl
        assertBool "native positive control allocation failed" (pointer /= nullPtr)
        writeIORef held (Just pointer)
        failConfig 1
        rejectControl pointer >>= \code -> assertBool "native rejection control was ineffective" (code /= 0)
        pure (Left ())
      oldRelease (Left ()) = pure ()
      oldRelease (Right pointer) = releaseControl pointer
  bracket_ (pure ()) cleanup $ do
    bracket oldAcquire oldRelease (const (pure ()))
    failConfig 0
    action
