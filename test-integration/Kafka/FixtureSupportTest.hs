module Kafka.FixtureSupportTest (tests) where

import Control.Concurrent (forkFinally, killThread)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import Control.Exception (IOException, bracket, throwIO, try)
import Data.IORef (IORef, atomicModifyIORef', mkWeakIORef, newIORef, readIORef, writeIORef)
import Foreign.StablePtr (StablePtr, freeStablePtr, newStablePtr)
import Kafka.CallbackTestSupport (assertRetained, awaitCollected)
import Kafka.Consumer (Offset (..))
import Kafka.FaultTestSupport (awaitBarrier, newBarrier, reachBarrier)
import Kafka.FixtureSupport
import Kafka.Producer
import System.Mem.Weak (Weak)
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertFailure, testCase, (@?=))

tests :: TestTree
tests =
  testGroup
    "Fixture error controls"
    [ testCase "immediate enqueue rejection propagates" $ do
        result <- sendAndConfirm 100000 (\_ _ -> pure (Left (ImmediateError failure))) (const (pure Nothing)) [record]
        result @?= Left failure,
      testCase "delivery failure propagates" $ do
        result <- sendAndConfirm 100000 (\r cb -> cb (DeliveryFailure r failure) >> pure (Right ())) (const (pure Nothing)) [record]
        result @?= Left failure,
      testCase "flush failure propagates" $ do
        result <- sendAndConfirm 100000 (\_ _ -> pure (Right ())) (const (pure (Just failure))) [record]
        result @?= Left failure,
      testCase "missing delivery callback fails" $ do
        result <- sendAndConfirm 1000 (\_ _ -> pure (Right ())) (const (pure Nothing)) [record]
        requireFailure result,
      testCase "wrong delivery identity fails" $ do
        result <- sendAndConfirm 100000 (\r cb -> cb (DeliverySuccess (r {prValue = Just "wrong"}) (Offset 0)) >> pure (Right ())) (const (pure Nothing)) [record]
        requireFailure result,
      testCase "matching delivery succeeds" $ do
        result <- sendAndConfirm 100000 (\r cb -> cb (DeliverySuccess r (Offset 0)) >> pure (Right ())) (const (pure Nothing)) [record]
        result @?= Right (),
      testCase "unexpected poll error propagates" $ do
        collect [Left failure] 1 >>= (@?= Left failure),
      testCase "missing record fails" $ do
        collect [Right 1] 2 >>= requireFailure,
      testCase "extra record fails" $ do
        collect [Right 1, Right 2] 1 >>= requireFailure,
      testCase "idle before exact records is retried" $ do
        collect [Left idle, Right 1, Left idle, Right 2] 2 >>= (@?= Right [1, 2]),
      testCase "partial setup releases first resource" $ do
        released <- newIORef False
        result <- try @IOException (acquirePair (pure ()) (const (writeIORef released True)) (throwIO (userError "injected setup failure") :: IO ()))
        case result of
          Left _ -> pure ()
          Right _ -> assertFailure "injected setup failure was swallowed"
        readIORef released >>= (@?= True),
      testCase "interrupted partial setup releases first resource" $ do
        released <- newIORef False
        stage <- newBarrier
        finished <- newEmptyMVar
        worker <-
          forkFinally
            (acquirePair (pure ()) (const (writeIORef released True)) (reachBarrier stage))
            (putMVar finished)
        awaitBarrier stage
        killThread worker
        takeMVar finished >>= \case
          Left _ -> pure ()
          Right _ -> assertFailure "interrupted acquisition unexpectedly completed"
        readIORef released >>= (@?= True),
      testCase "weak control retains while rooted and collects after release" $
        do
          weak <- bracket capturedRoot (freeStablePtr . fst) $ \(_, weak) -> do
            assertRetained weak
            pure weak
          awaitCollected weak
    ]
  where
    failure = KafkaError "injected failure"
    idle = KafkaResponseError RdKafkaRespErrTimedOut
    record = ProducerRecord (TopicName "fixture") (SpecifiedPartition 0) Nothing (Just "payload") mempty
    collect :: [Either KafkaError Int] -> Int -> IO (Either KafkaError [Int])
    collect events count = do
      remaining <- newIORef events
      ticks <- newIORef 0
      let clock = atomicModifyIORef' ticks (\n -> (n + 1, n))
          poll = atomicModifyIORef' remaining $ \case
            [] -> ([], Left idle)
            e : es -> (es, e)
      collectExactly clock 20 count poll
    requireFailure :: (Show a) => Either KafkaError a -> IO ()
    requireFailure = \case
      Left _ -> pure ()
      Right value -> assertFailure ("expected failure, got " <> show value)

{-# NOINLINE capturedRoot #-}
capturedRoot :: IO (StablePtr (IO Int), Weak (IORef Int))
capturedRoot = do
  value <- newIORef (42 :: Int)
  weak <- mkWeakIORef value (pure ())
  root <- newStablePtr (readIORef value)
  pure (root, weak)
