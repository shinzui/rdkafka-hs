module Kafka.ConsumerStateSupport (withStateFault, endStateFault, stateStat, triggerRebalance, stateLeakControl, commitObservation) where

import Control.Exception (bracket_)
import Data.Text qualified as Text
import Foreign.C.String (CString, withCString)
import Foreign.C.Types (CInt (..))
import Kafka.Types (TopicName (..))

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
