module Kafka.Integration.CallbackLifetimeTest (tests) where

import Control.Concurrent (ThreadId, forkFinally, forkIOWithUnmask, killThread, threadDelay)
import Control.Concurrent.MVar (MVar, newEmptyMVar, putMVar, readMVar, tryPutMVar, tryReadMVar)
import Control.Exception (AsyncException (ThreadKilled), SomeException, finally, fromException, throwIO, try)
import Control.Monad (void)
import Data.IORef (IORef, atomicModifyIORef', mkWeakIORef, modifyIORef', newIORef, readIORef, writeIORef)
import Data.Map.Strict qualified as M
import Data.Text qualified as Text
import GHC.Conc (BlockReason (BlockedOnException, BlockedOnMVar), ThreadStatus (ThreadBlocked), threadStatus)
import Kafka.CallbackLifetimeSupport
import Kafka.CallbackTestSupport (assertRetained, awaitCollected)
import Kafka.Consumer qualified as C
import Kafka.Producer qualified as P
import Kafka.TestEnv
import System.Mem.Weak (Weak)
import System.Timeout (timeout)
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertBool, assertFailure, testCase, (@?=))

tests :: TestEnv -> TestTree
tests env =
  testGroup
    "callback-lifetime"
    ( [ testCase (label <> " error callback releases after destroy with retained wrapper") $ do
          (consumer, capture) <- consumerProbe env mode label
          awaitCollected capture
          -- Keep the destroyed wrapper live through the collection assertion.
          C.closeConsumer consumer >>= (@?= Nothing)
      | (label, mode) <- [("async", C.CallbackPollModeAsync), ("sync", C.CallbackPollModeSync)]
      ]
        <> [ testCase "producer delivery callback releases after native finalization" $
               withTopic env "callback-owner-producer" 1 $ \topic _ ->
                 producerProbe env topic >>= awaitCollected,
             testCase "noncaptured control is collectible" $ control >>= awaitCollected
           ]
        <> [ testCase (label <> " " <> kind <> " callback is live then releases") $ do
               (consumer, weak) <- consumerKindProbe env mode label kind
               awaitCollected weak
               C.closeConsumer consumer >>= (@?= Nothing)
           | (label, mode) <- [("async", C.CallbackPollModeAsync), ("sync", C.CallbackPollModeSync)],
             kind <- ["log", "stats"]
           ]
        <> [ testCase (label <> " construction failure releases callbacks: " <> stage) $
               failedConsumerProbe env mode label stage >>= awaitCollected
           | (label, mode) <- [("async", C.CallbackPollModeAsync), ("sync", C.CallbackPollModeSync)],
             stage <- ["topic", "native"]
           ]
        <> [ testCase "producer native construction failure releases callbacks" $
               failedProducerProbe env >>= awaitCollected
           ]
        <> [ testCase (label <> " " <> kind <> " callback releases captured borrowed configuration") $
               withTopic env (Text.pack ("owner-" <> label <> "-" <> kind)) 1 $ \topic producer -> do
                 produceTo producer topic 0 "owner-record"
                 (consumer, weak) <- consumerStateProbe env mode label kind topic
                 awaitCollected weak
                 C.closeConsumer consumer >>= (@?= Nothing)
           | (label, mode) <- [("async", C.CallbackPollModeAsync), ("sync", C.CallbackPollModeSync)],
             kind <- ["rebalance", "commit"]
           ]
        <> [ testCase "replaced producer delivery callbacks both release" $
               withTopic env "owner-replaced-delivery" 1 $ \topic _ -> do
                 (first, second) <- replacedProducerProbe env topic
                 awaitCollected first
                 awaitCollected second
           ]
        <> [ testCase (label <> " concurrent destruction joins at " <> stage) $
               withCallbackTracking fault (destroyStage env mode label fault)
           | (label, mode) <- [("async", C.CallbackPollModeAsync), ("sync", C.CallbackPollModeSync)],
             (stage, fault) <- [("native entry", 3), ("native completion", 5), ("native shutdown callback", 6)]
           ]
        <> [ testCase (label <> " cancellation at " <> stage <> " preserves exactly-once teardown") $
               withCallbackTracking fault (cancelDestroyStage env mode label fault)
           | (label, mode) <- [("async", C.CallbackPollModeAsync), ("sync", C.CallbackPollModeSync)],
             (stage, fault) <- [("native entry", 3), ("native completion", 5), ("native shutdown callback", 6)]
           ]
        <> [ testCase "async cancellation after poll stop and queue detachment preserves teardown" $
               withCallbackTracking 4 (cancelDestroyStage env C.CallbackPollModeAsync "async-queue" 4)
           ]
        <> [ testCase (role <> " constructor cancellation at " <> stage <> " releases its owner") $
               withCallbackTracking fault $ do
                 weak <- cancelledConstructor env role fault
                 awaitCollected weak
                 let created = if fault == 2 then 1 else 0
                 callbackStat 1 >>= (@?= created)
                 callbackStat 2 >>= (@?= created)
                 callbackStat 3 >>= (@?= created)
                 callbackStat 6 >>= (@?= 1)
           | role <- ["producer", "consumer-async", "consumer-sync"],
             (stage, fault) <- [("partial registration", 1), ("native acceptance", 2)]
           ]
        <> [ testCase (label <> " cancellation while waiting for active callback permits retry") $
               withCallbackTracking 0 (cancelWhilePolling env mode label)
           | (label, mode) <- [("async", C.CallbackPollModeAsync), ("sync", C.CallbackPollModeSync)]
           ]
        <> [ testCase (label <> " closed consumer GC releases its registrations once") $
               withCallbackTracking 0 $ do
                 closedConsumerProbe env mode label >>= awaitCollected
                 callbackStat 1 >>= (@?= 1)
                 callbackStat 2 >>= (@?= 1)
                 callbackStat 3 >>= (@?= 1)
           | (label, mode) <- [("async", C.CallbackPollModeAsync), ("sync", C.CallbackPollModeSync)]
           ]
        <> [ testCase ("producer " <> kind <> " callback remains live through close and releases on GC") $
               withCallbackTracking 0 $ do
                 producerKindProbe env kind >>= awaitCollected
                 callbackStat 1 >>= (@?= 1)
                 callbackStat 2 >>= (@?= 1)
                 callbackStat 3 >>= (@?= 1)
           | kind <- ["error", "log", "stats"]
           ]
        <> [ testCase "failed native construction releases the caller-owned duplicate config" $
               withCallbackTracking 0 $ do
                 failedProducerProbe env >>= awaitCollected
                 callbackStat 1 >>= (@?= 0)
                 callbackStat 2 >>= (@?= 0)
                 callbackStat 7 >>= (@?= 1)
                 callbackStat 5 >>= (@?= 1)
           ]
    )

{-# NOINLINE control #-}
control :: IO (Weak (IORef Int))
control = newIORef 0 >>= \value -> mkWeakIORef value (pure ())

{-# NOINLINE consumerProbe #-}
consumerProbe :: TestEnv -> C.CallbackPollMode -> String -> IO (C.KafkaConsumer, Weak (IORef Int))
consumerProbe env mode label = do
  capture <- newIORef 0
  weak <- mkWeakIORef capture (pure ())
  called <- newEmptyMVar
  consumer <-
    C.newAssignedConsumer
      ( C.brokersList ["127.0.0.1:1"]
          <> C.groupId (makeGroupId env (Text.pack ("callback-owner-" <> label)))
          <> C.noAutoCommit
          <> C.callbackPollMode mode
          <> C.setCallback (C.errorCallback (\_ _ -> atomicModifyIORef' capture (\n -> (n + 1, ())) >> void (tryPutMVar called ())))
      )
      >>= either (assertFailure . show) pure
  flip finally (C.destroyConsumer consumer) $ do
    assertRetained weak
    awaitCalled consumer called
    readIORef capture >>= \count -> if count > 0 then pure () else assertFailure "native error callback never ran"
  pure (consumer, weak)

awaitCalled :: C.KafkaConsumer -> MVar () -> IO ()
awaitCalled consumer called = timeout 5000000 loop >>= maybe (assertFailure "native callback was not observed") pure
  where
    loop =
      tryReadMVar called >>= \case
        Just () -> pure ()
        Nothing -> C.pollConsumerEvents consumer (Just (C.Timeout 50)) >> loop

{-# NOINLINE producerProbe #-}
producerProbe :: TestEnv -> P.TopicName -> IO (Weak (IORef Int))
producerProbe env topic = do
  capture <- newIORef 0
  weak <- mkWeakIORef capture (pure ())
  producer <-
    P.newProducer
      (P.brokersList (teBrokers env) <> P.setCallback (P.deliveryCallback (\_ -> atomicModifyIORef' capture (\n -> (n + 1, ())))))
      >>= either (assertFailure . show) pure
  assertRetained weak
  sendMessages [P.ProducerRecord topic (P.SpecifiedPartition 0) Nothing (Just "callback-owner") mempty] producer
    >>= (@?= Right ())
  readIORef capture >>= (@?= 1)
  P.closeProducer producer
  -- Close flushes; it is not a destruction boundary.
  assertRetained weak
  P.flushProducerTimeout producer (P.Timeout 0) >>= (@?= Nothing)
  pure weak

{-# NOINLINE consumerKindProbe #-}
consumerKindProbe :: TestEnv -> C.CallbackPollMode -> String -> String -> IO (C.KafkaConsumer, Weak (IORef Int))
consumerKindProbe env mode label kind = do
  capture <- newIORef 0
  weak <- mkWeakIORef capture (pure ())
  called <- newEmptyMVar
  let observed = atomicModifyIORef' capture (\n -> (n + 1, ())) >> void (tryPutMVar called ())
      callback = if kind == "log" then C.logCallback (\_ _ _ -> observed) else C.statsCallback (const observed)
      extras = if kind == "log" then M.singleton "debug" "cgrp,topic" else M.singleton "statistics.interval.ms" "100"
  consumer <-
    C.newAssignedConsumer
      ( C.brokersList (teBrokers env)
          <> C.groupId (makeGroupId env (Text.pack ("owner-" <> label <> "-" <> kind)))
          <> C.noAutoCommit
          <> C.callbackPollMode mode
          <> C.extraProps extras
          <> C.setCallback callback
      )
      >>= either (assertFailure . show) pure
  flip finally (C.destroyConsumer consumer) $ do
    assertRetained weak
    awaitCalled consumer called
    readIORef capture >>= \count -> assertBool "native callback ran" (count > 0)
    C.closeConsumer consumer >>= (@?= Nothing)
    assertRetained weak
  pure (consumer, weak)

{-# NOINLINE failedConsumerProbe #-}
failedConsumerProbe :: TestEnv -> C.CallbackPollMode -> String -> String -> IO (Weak (IORef Int))
failedConsumerProbe env mode label stage = do
  capture <- newIORef 0
  weak <- mkWeakIORef capture (pure ())
  let props =
        C.brokersList (teBrokers env)
          <> C.groupId (makeGroupId env (Text.pack ("owner-failure-" <> label <> "-" <> stage)))
          <> C.noAutoCommit
          <> C.callbackPollMode mode
          <> C.setCallback (C.errorCallback (\_ _ -> atomicModifyIORef' capture (\n -> (n + 1, ()))))
      invalid = C.extraProps (M.fromList [("security.protocol", "sasl_plaintext"), ("sasl.mechanism", "PLAIN")])
      sub = C.extraSubscriptionProps (M.singleton "__ownership.invalid" "x")
  result <- try @C.KafkaError $ if stage == "topic" then C.newConsumer props sub else C.newAssignedConsumer (props <> invalid)
  case result of
    Left (C.KafkaUnknownConfigurationKey message)
      | stage == "topic" ->
          assertBool (Text.unpack message) ("__ownership.invalid" `Text.isInfixOf` message)
    Right (Left (C.KafkaError message))
      | stage == "native" ->
          assertBool (Text.unpack message) ("sasl.username and sasl.password must be set" `Text.isInfixOf` message)
    Right (Right consumer) -> C.destroyConsumer consumer >> assertFailure "invalid constructor succeeded"
    other -> assertFailure ("unexpected constructor result: " <> case other of Left e -> show e; Right (Left e) -> show e)
  pure weak

{-# NOINLINE failedProducerProbe #-}
failedProducerProbe :: TestEnv -> IO (Weak (IORef Int))
failedProducerProbe env = do
  capture <- newIORef 0
  weak <- mkWeakIORef capture (pure ())
  result <-
    P.newProducer
      ( P.brokersList (teBrokers env)
          <> P.extraProps (M.fromList [("security.protocol", "sasl_plaintext"), ("sasl.mechanism", "PLAIN")])
          <> P.setCallback (P.deliveryCallback (\_ -> atomicModifyIORef' capture (\n -> (n + 1, ()))))
      )
  case result of
    Left (P.KafkaError message) -> assertBool (Text.unpack message) ("sasl.username and sasl.password must be set" `Text.isInfixOf` message)
    Left other -> assertFailure (show other)
    Right producer -> releaseProducer producer >> assertFailure "invalid native producer succeeded"
  pure weak

{-# NOINLINE consumerStateProbe #-}
consumerStateProbe :: TestEnv -> C.CallbackPollMode -> String -> String -> C.TopicName -> IO (C.KafkaConsumer, Weak (IORef (Maybe C.KafkaConsumer)))
consumerStateProbe env mode label kind topic = do
  capture <- newIORef Nothing
  weak <- mkWeakIORef capture (pure ())
  events <- newIORef []
  called <- newEmptyMVar
  committed <- newEmptyMVar
  let onRebalance borrowed event = do
        writeIORef capture (Just borrowed)
        modifyIORef' events (<> [event])
        case event of
          C.RebalanceAssign _ -> void (tryPutMVar called ())
          _ -> pure ()
      onCommit borrowed err partitions = do
        writeIORef capture (Just borrowed)
        void (tryPutMVar committed (err, partitions))
        void (tryPutMVar called ())
      callback = if kind == "rebalance" then C.rebalanceCallback onRebalance else C.offsetCommitCallback onCommit
      props =
        C.brokersList (teBrokers env)
          <> C.groupId (makeGroupId env (Text.pack ("owner-state-" <> label <> "-" <> kind)))
          <> C.noAutoCommit
          <> C.callbackPollMode mode
          <> C.setCallback callback
      sub = if kind == "rebalance" then C.topics [topic] <> C.offsetReset C.Earliest else mempty
  consumer <- C.newConsumer props sub >>= either (assertFailure . show) pure
  flip finally (releaseConsumer consumer) $ do
    assertRetained weak
    if kind == "commit" then C.assign consumer [C.TopicPartition topic (C.PartitionId 0) (C.PartitionOffset 0)] >>= (@?= Nothing) else pure ()
    record <- C.pollMessage consumer (C.Timeout 10000) >>= either (assertFailure . show) pure
    (C.crTopic record, C.crPartition record, C.crOffset record, C.crValue record)
      @?= (topic, C.PartitionId 0, C.Offset 0, Just "owner-record")
    if kind == "commit" then C.commitOffsetMessage C.OffsetCommit consumer record >>= (@?= Nothing) else pure ()
    awaitCalled consumer called
    if kind == "commit"
      then readMVar committed >>= (@?= (C.KafkaResponseError C.RdKafkaRespErrNoError, [C.TopicPartition topic (C.PartitionId 0) (C.PartitionOffset 1)]))
      else readIORef events >>= (@?= [C.RebalanceBeforeAssign [(topic, C.PartitionId 0)], C.RebalanceAssign [(topic, C.PartitionId 0)]])
    C.closeConsumer consumer >>= (@?= Nothing)
    assertRetained weak
    if kind == "rebalance"
      then readIORef events >>= (@?= [C.RebalanceBeforeAssign [(topic, C.PartitionId 0)], C.RebalanceAssign [(topic, C.PartitionId 0)], C.RebalanceBeforeRevoke [(topic, C.PartitionId 0)], C.RebalanceRevoke [(topic, C.PartitionId 0)]])
      else pure ()
  pure (consumer, weak)

{-# NOINLINE replacedProducerProbe #-}
replacedProducerProbe :: TestEnv -> P.TopicName -> IO (Weak (IORef Int), Weak (IORef Int))
replacedProducerProbe env topic = do
  first <- newIORef 0
  second <- newIORef 0
  weakFirst <- mkWeakIORef first (pure ())
  weakSecond <- mkWeakIORef second (pure ())
  producer <-
    P.newProducer
      ( P.brokersList (teBrokers env)
          <> P.setCallback (P.deliveryCallback (\_ -> modifyIORef' first (+ 1)))
          <> P.setCallback (P.deliveryCallback (\_ -> modifyIORef' second (+ 1)))
      )
      >>= either (assertFailure . show) pure
  flip finally (releaseProducer producer) $ do
    assertRetained weakFirst
    assertRetained weakSecond
    sendMessages [P.ProducerRecord topic (P.SpecifiedPartition 0) Nothing (Just "replacement") mempty] producer >>= (@?= Right ())
    readIORef first >>= (@?= 0)
    readIORef second >>= (@?= 1)
  pure (weakFirst, weakSecond)

{-# NOINLINE trackedConsumer #-}
trackedConsumer :: TestEnv -> C.CallbackPollMode -> String -> IO (C.KafkaConsumer, Weak (IORef Int), MVar ())
trackedConsumer env mode label = do
  capture <- newIORef 0
  weak <- mkWeakIORef capture (pure ())
  called <- newEmptyMVar
  let callback _ facility _ = do
        atomicModifyIORef' capture (\n -> (n + 1, ()))
        if facility == "DESTROY" then void (tryPutMVar called ()) else pure ()
  consumer <-
    C.newAssignedConsumer
      ( C.brokersList (teBrokers env)
          <> C.groupId (makeGroupId env (Text.pack ("owner-destroy-" <> label)))
          <> C.noAutoCommit
          <> C.callbackPollMode mode
          <> C.extraProps (M.singleton "debug" "generic,topic,cgrp")
          <> C.setCallback (C.logCallback callback)
      )
      >>= either (assertFailure . show) pure
  pure (consumer, weak, called)

destroyStage :: TestEnv -> C.CallbackPollMode -> String -> Int -> IO ()
destroyStage env mode label fault = do
  (consumer, weak, logged) <- trackedConsumer env mode (label <> show fault)
  first <- newEmptyMVar
  void $ forkFinally (C.destroyConsumer consumer) (putMVar first)
  flip finally (releaseCallbackStage >> bounded (readMVar first) >>= either throwIO pure) $ do
    awaitCallbackStage
    callbackStat 1 >>= (@?= 1)
    callbackStat 2 >>= (@?= 1)
    callbackStat 3 >>= (@?= if fault == 5 then 1 else 0)
    if fault == 6 then bounded (readMVar logged) else pure ()
    assertRetained weak
    second <- newEmptyMVar
    secondThread <- forkFinally (C.destroyConsumer consumer) (putMVar second)
    flip finally (releaseCallbackStage >> bounded (readMVar second) >>= either throwIO pure) $ do
      let awaitBlocked =
            threadStatus secondThread >>= \status ->
              if status == ThreadBlocked BlockedOnMVar then pure () else threadDelay 1000 >> awaitBlocked
      bounded awaitBlocked
      tryReadMVar first >>= maybe (pure ()) (const (assertFailure "first destroy returned while native stage held"))
      tryReadMVar second >>= maybe (pure ()) (const (assertFailure "concurrent destroy returned while native stage held"))
      releaseCallbackStage
      bounded (readMVar first) >>= either throwIO pure
      bounded (readMVar second) >>= either throwIO pure
    awaitCollected weak
    callbackStat 2 >>= (@?= 1)
    callbackStat 3 >>= (@?= 1)
    callbackStat 4 >>= \count -> assertBool "real native shutdown called the Haskell log bridge" (count > 0)
    C.destroyConsumer consumer
    C.closeConsumer consumer >>= (@?= Nothing)
    callbackStat 2 >>= (@?= 1)

bounded :: IO a -> IO a
bounded action = timeout 10000000 action >>= maybe (assertFailure "callback lifetime stage timed out") pure

cancelDestroyStage :: TestEnv -> C.CallbackPollMode -> String -> Int -> IO ()
cancelDestroyStage env mode label fault = do
  (consumer, weak, _) <- trackedConsumer env mode (label <> "-cancel-" <> show fault)
  done <- newEmptyMVar
  worker <- forkUnmaskedFinally (C.destroyConsumer consumer) (putMVar done)
  flip finally (releaseCallbackStage >> bounded (readMVar done) >> C.destroyConsumer consumer) $ do
    awaitCallbackStage
    assertRetained weak
    if fault == 4 then callbackStat 8 >>= (@?= 1) else callbackStat 2 >>= (@?= 1)
    killed <- newEmptyMVar
    killer <- forkFinally (killThread worker) (putMVar killed)
    let awaitPending =
          threadStatus killer >>= \status ->
            if status == ThreadBlocked BlockedOnException then pure () else threadDelay 1000 >> awaitPending
    bounded awaitPending
    tryReadMVar done >>= maybe (pure ()) (const (assertFailure "destroy returned before cancellation stage released"))
    releaseCallbackStage
    result <- bounded (readMVar done)
    case result of
      Left exception -> (fromException exception :: Maybe AsyncException) @?= Just ThreadKilled
      Right () -> assertFailure "observed pending cancellation was not delivered"
    bounded (readMVar killed) >>= either throwIO pure
    C.destroyConsumer consumer
    awaitCollected weak
    callbackStat 1 >>= (@?= 1)
    callbackStat 2 >>= (@?= 1)
    callbackStat 3 >>= (@?= 1)
    C.closeConsumer consumer >>= (@?= Nothing)

-- Tasty may enter a case masked. Restore an explicitly unmasked worker rather
-- than inheriting the controller's masking state through forkFinally.
forkUnmaskedFinally :: IO a -> (Either SomeException a -> IO ()) -> IO ThreadId
forkUnmaskedFinally action completed = forkIOWithUnmask $ \unmask ->
  try @SomeException (unmask action) >>= completed

{-# NOINLINE cancelledConstructor #-}
cancelledConstructor :: TestEnv -> String -> Int -> IO (Weak (IORef Int))
cancelledConstructor env role fault = do
  capture <- newIORef 0
  weak <- mkWeakIORef capture (pure ())
  let observed _ = atomicModifyIORef' capture (\n -> (n + 1, ()))
      construct
        | role == "producer" =
            P.newProducer
              (P.brokersList (teBrokers env) <> P.setCallback (P.statsCallback observed))
              >>= either throwIO releaseProducer
        | otherwise =
            C.newAssignedConsumer
              ( C.brokersList (teBrokers env)
                  <> C.groupId (makeGroupId env (Text.pack ("owner-cancel-" <> role <> show fault)))
                  <> C.noAutoCommit
                  <> C.callbackPollMode (if role == "consumer-async" then C.CallbackPollModeAsync else C.CallbackPollModeSync)
                  <> C.setCallback (C.statsCallback observed)
              )
              >>= either throwIO C.destroyConsumer
  done <- newEmptyMVar
  worker <- forkUnmaskedFinally construct (putMVar done)
  flip finally (releaseCallbackStage >> bounded (readMVar done) >> pure ()) $ do
    awaitCallbackStage
    assertRetained weak
    killed <- newEmptyMVar
    killer <- forkFinally (killThread worker) (putMVar killed)
    let awaitPending =
          threadStatus killer >>= \status ->
            if status == ThreadBlocked BlockedOnException then pure () else threadDelay 1000 >> awaitPending
    bounded awaitPending
    releaseCallbackStage
    bounded (readMVar done) >>= \case
      Left exception -> (fromException exception :: Maybe AsyncException) @?= Just ThreadKilled
      Right () -> assertFailure "constructor escaped an observed pending cancellation"
    bounded (readMVar killed) >>= either throwIO pure
  pure weak

{-# NOINLINE closedConsumerProbe #-}
closedConsumerProbe :: TestEnv -> C.CallbackPollMode -> String -> IO (Weak (IORef Int))
closedConsumerProbe env mode label = do
  (consumer, weak, _) <- trackedConsumer env mode (label <> "-closed-gc")
  C.closeConsumer consumer >>= (@?= Nothing)
  assertRetained weak
  callbackStat 2 >>= (@?= 0)
  C.consumerClosed consumer >>= (@?= True)
  pure weak

{-# NOINLINE producerKindProbe #-}
producerKindProbe :: TestEnv -> String -> IO (Weak (IORef Int))
producerKindProbe env kind = do
  capture <- newIORef 0
  weak <- mkWeakIORef capture (pure ())
  called <- newEmptyMVar
  let observed = atomicModifyIORef' capture (\n -> (n + 1, ())) >> void (tryPutMVar called ())
      callback = case kind of
        "log" -> P.logCallback (\_ _ _ -> observed)
        "stats" -> P.statsCallback (const observed)
        _ -> P.errorCallback (\_ _ -> observed)
      extras = if kind == "log" then M.singleton "debug" "generic" else M.singleton "statistics.interval.ms" "100"
  producer <-
    P.newProducer
      ( P.brokersList ["127.0.0.1:1"]
          <> P.sendTimeout (P.Timeout 1000)
          <> P.extraProps extras
          <> P.setCallback callback
      )
      >>= either (assertFailure . show) pure
  flip finally (releaseProducer producer) $ do
    let record = P.ProducerRecord (P.TopicName (prefixedName env (Text.pack ("owner-producer-" <> kind)))) P.UnassignedPartition Nothing (Just "pending") mempty
    P.produceMessage producer record >>= (@?= Nothing)
    let awaitEvent =
          tryReadMVar called >>= \case
            Just () -> pure ()
            Nothing -> do
              P.flushProducerTimeout producer (P.Timeout 100) >>= \case
                Nothing -> pure ()
                Just (P.KafkaResponseError P.RdKafkaRespErrTimedOut) -> pure ()
                Just err -> assertFailure (show err)
              awaitEvent
    bounded awaitEvent
    assertRetained weak
    P.purgeProducer producer [P.PurgeQueue, P.PurgeInFlight] >>= (@?= Nothing)
    P.flushProducerTimeout producer (P.Timeout 5000) >>= (@?= Nothing)
    P.closeProducer producer
    assertRetained weak
  pure weak

{-# NOINLINE lockedConsumer #-}
lockedConsumer :: TestEnv -> C.CallbackPollMode -> String -> IO (C.KafkaConsumer, Weak (IORef Int), MVar (), MVar ())
lockedConsumer env mode label = do
  capture <- newIORef 0
  weak <- mkWeakIORef capture (pure ())
  reached <- newEmptyMVar
  resume <- newEmptyMVar
  let onError _ _ = do
        atomicModifyIORef' capture (\n -> (n + 1, ()))
        void (tryPutMVar reached ())
        bounded (readMVar resume)
  consumer <-
    C.newAssignedConsumer
      ( C.brokersList ["127.0.0.1:1"]
          <> C.groupId (makeGroupId env (Text.pack ("owner-locked-" <> label)))
          <> C.noAutoCommit
          <> C.callbackPollMode mode
          <> C.setCallback (C.errorCallback onError)
      )
      >>= either (assertFailure . show) pure
  pure (consumer, weak, reached, resume)

cancelWhilePolling :: TestEnv -> C.CallbackPollMode -> String -> IO ()
cancelWhilePolling env mode label = do
  (consumer, weak, reached, resume) <- lockedConsumer env mode label
  polled <- newEmptyMVar
  let pollUntilReached =
        tryReadMVar reached >>= \case
          Just () -> pure ()
          Nothing -> C.pollConsumerEvents consumer (Just (C.Timeout 100)) >> pollUntilReached
      release = void (tryPutMVar resume ())
  void $ forkUnmaskedFinally pollUntilReached (putMVar polled)
  flip finally (release >> bounded (readMVar polled) >> C.destroyConsumer consumer) $ do
    bounded (readMVar reached)
    done <- newEmptyMVar
    worker <- forkUnmaskedFinally (C.destroyConsumer consumer) (putMVar done)
    let awaitBlocked =
          threadStatus worker >>= \status ->
            if status == ThreadBlocked BlockedOnMVar then pure () else threadDelay 1000 >> awaitBlocked
    bounded awaitBlocked
    killThread worker
    bounded (readMVar done) >>= \case
      Left exception -> (fromException exception :: Maybe AsyncException) @?= Just ThreadKilled
      Right () -> assertFailure "destroy did not wait for the active callback"
    callbackStat 2 >>= (@?= 0)
    assertRetained weak
    release
    bounded (readMVar polled) >>= either throwIO pure
    C.destroyConsumer consumer
    awaitCollected weak
    callbackStat 2 >>= (@?= 1)
    callbackStat 3 >>= (@?= 1)
    C.closeConsumer consumer >>= (@?= Nothing)
