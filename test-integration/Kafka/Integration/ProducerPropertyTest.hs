module Kafka.Integration.ProducerPropertyTest (tests) where

import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import Control.Exception (bracket, throwIO)
import Data.IORef (atomicModifyIORef', newIORef, readIORef)
import Kafka.Producer
import Kafka.PropertySupport
import Kafka.TestEnv (releaseProducer)
import System.Mem (performMajorGC)
import System.Timeout (timeout)
import Test.Tasty (TestTree)
import Test.Tasty.HUnit (assertFailure, (@?=))
import Test.Tasty.QuickCheck qualified as QC

tests :: TestTree
tests =
  generatedGroup
    "producer-properties"
    [ QC.testProperty "callback composition preserves order and identity" $
        QC.forAllShrink programs shrinkPrograms $ \input@(a, b, c, leftAssociated) -> QC.ioProperty $ do
          result <- observeCallbacks a b c leftAssociated
          -- The helper has returned, so this GC does not root a producer on the
          -- property's stack while subsequent native cases allocate clients.
          performMajorGC
          let labels = a <> b <> c
              expected = case reverse labels of [] -> []; lastLabel : _ -> [(lastLabel, expectedReport)]
          pure $ QC.counterexample (show input) (result QC.=== (length labels, expected, expectedReport))
    ]

programs :: QC.Gen ([Int], [Int], [Int], Bool)
programs = (,,,) <$> labels <*> labels <*> labels <*> QC.elements [False, True]
  where
    labels = boundedList 6 (QC.choose (0, 5))

shrinkPrograms :: ([Int], [Int], [Int], Bool) -> [([Int], [Int], [Int], Bool)]
shrinkPrograms (a, b, c, association) =
  [(a', b, c, association) | a' <- QC.shrinkList QC.shrink a]
    <> [(a, b', c, association) | b' <- QC.shrinkList QC.shrink b]
    <> [(a, b, c', association) | c' <- QC.shrinkList QC.shrink c]
    <> [(a, b, c, False) | association]

record :: ProducerRecord
record = ProducerRecord (TopicName "producer-property-callbacks") (SpecifiedPartition 0) Nothing (Just "generated callback") mempty

expectedReport :: DeliveryReport
expectedReport = DeliveryFailure record (KafkaResponseError RdKafkaRespErrPurgeQueue)

{-# NOINLINE observeCallbacks #-}
observeCallbacks :: [Int] -> [Int] -> [Int] -> Bool -> IO (Int, [(Int, DeliveryReport)], DeliveryReport)
observeCallbacks a b c leftAssociated = do
  observed <- newIORef []
  done <- newEmptyMVar
  let callbacks = foldMap $ \label -> setCallback (deliveryCallback (\report -> atomicModifyIORef' observed (\reports -> (reports <> [(label, report)], ()))))
      p = callbacks a
      q = callbacks b
      r = callbacks c
      composed = if leftAssociated then ((p <> mempty) <> q) <> r else p <> (mempty <> (q <> r))
      props = brokersList [BrokerAddress "127.0.0.1:1"] <> extraProp "log_level" "0" <> composed
  bracket (newProducer props >>= either throwIO pure) releaseProducer $ \producer -> do
    produceMessage' producer record (putMVar done) >>= (@?= Right ())
    purgeProducer producer [PurgeQueue] >>= (@?= Nothing)
    flushProducerTimeout producer (Timeout 1000) >>= (@?= Nothing)
    report <- timeout 5000000 (takeMVar done) >>= maybe (assertFailure "generated callback did not complete") pure
    reports <- readIORef observed
    pure (length (ppCallbacks composed), reports, report)
