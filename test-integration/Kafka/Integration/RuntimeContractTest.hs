{-# OPTIONS_GHC -Wno-deprecations #-}

module Kafka.Integration.RuntimeContractTest (tests) where

import Control.Concurrent (rtsSupportsBoundThreads)
import Control.Exception (bracket, finally, throwIO)
import Data.IORef (newIORef, readIORef, writeIORef)
import Kafka.Consumer
import Kafka.TestEnv
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertBool, assertFailure, testCase, (@?=))

tests :: TestEnv -> TestTree
tests env =
  testGroup
    "runtime-contract"
    [ testCase "RTS capability is observed by async construction" $ do
        result <- newAssignedConsumer (base "async")
        if rtsSupportsBoundThreads
          then either throwIO destroyConsumer result
          else case result of
            Left (KafkaBadSpecification message) -> assertBool (show message) (message == "Async callback polling requires -threaded; select CallbackPollModeSync for a nonthreaded runtime.")
            Left err -> assertFailure (show err)
            Right consumer -> destroyConsumer consumer >> assertFailure "nonthreaded async construction silently succeeded",
      testCase "explicit async construction checks the runtime" $ do
        result <- newAssignedConsumer (base "explicit-async" <> callbackPollMode CallbackPollModeAsync)
        if rtsSupportsBoundThreads
          then either throwIO destroyConsumer result
          else case result of
            Left (KafkaBadSpecification _) -> pure ()
            Left err -> assertFailure (show err)
            Right consumer -> destroyConsumer consumer >> assertFailure "explicit nonthreaded async construction silently succeeded",
      testCase "sync subscription delivers an exact record" $
        withTopic env "runtime-sync" 1 $ \topic producer -> do
          produceTo producer topic 0 "runtime-record"
          bracket (newConsumer (base "sync" <> callbackPollMode CallbackPollModeSync) (topics [topic] <> offsetReset Earliest) >>= either throwIO pure) releaseConsumer $ \consumer -> do
            message <- pollMessage consumer (Timeout 10000) >>= either throwIO pure
            (crTopic message, crPartition message, crOffset message, crValue message) @?= (topic, PartitionId 0, Offset 0, Just "runtime-record"),
      testCase "deprecated sync runner closes after an exact record" $
        withTopic env "runtime-runner" 1 $ \topic producer -> do
          produceTo producer topic 0 "runner-record"
          retained <- newIORef Nothing
          let cleanup = readIORef retained >>= mapM_ destroyConsumer
          flip finally cleanup $ do
            result <- runConsumer (base "runner" <> callbackPollMode CallbackPollModeSync) (topics [topic] <> offsetReset Earliest) $ \consumer -> do
              writeIORef retained (Just consumer)
              fmap (fmap (\message -> (crOffset message, crValue message))) (pollMessage consumer (Timeout 10000))
            result @?= Right (Offset 0, Just "runner-record")
            readIORef retained >>= maybe (assertFailure "runner did not invoke its handler") (\consumer -> consumerClosed consumer >>= (@?= True))
    ]
  where
    base name = brokersList (teBrokers env) <> groupId (makeGroupId env ("runtime-" <> name)) <> noAutoCommit
