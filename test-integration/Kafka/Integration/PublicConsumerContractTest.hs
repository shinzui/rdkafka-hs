module Kafka.Integration.PublicConsumerContractTest (tests, runRebalanceProbe) where

import Control.Concurrent (forkFinally, forkIOWithUnmask, killThread, threadDelay)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, readMVar, tryReadMVar)
import Control.Exception (AsyncException (ThreadKilled), ErrorCall, SomeException, bracket, finally, fromException, throwIO, try)
import Control.Monad (forM_)
import Data.Either (isLeft)
import Data.IORef (modifyIORef', newIORef, readIORef)
import Data.Map.Strict qualified as M
import Data.Maybe (isJust)
import Data.Text qualified as Text
import GHC.Clock (getMonotonicTimeNSec)
import GHC.Conc (BlockReason (BlockedOnException), ThreadStatus (ThreadBlocked), getUncaughtExceptionHandler, setUncaughtExceptionHandler, threadStatus)
import Kafka.CallbackTestSupport (runChildProbe)
import Kafka.Consumer
import Kafka.ConsumerStateSupport
import Kafka.Integration.ConfigStringContractTest qualified as ConfigStringContractTest
import Kafka.Integration.ConsumerCommitContractTest qualified as ConsumerCommitContractTest
import Kafka.Integration.ConsumerStateContractTest qualified as ConsumerStateContractTest
import Kafka.Integration.DeprecatedRunnerContractTest qualified as DeprecatedRunnerContractTest
import Kafka.Integration.DiagnosticContractTest qualified as DiagnosticContractTest
import Kafka.Integration.FatalErrorTest qualified as FatalErrorTest
import Kafka.Integration.InputBoundaryContractTest qualified as InputBoundaryContractTest
import Kafka.Integration.MetadataContractTest qualified as MetadataContractTest
import Kafka.Integration.PartitionListContractTest qualified as PartitionListContractTest
import Kafka.Integration.QueueEventTest qualified as QueueEventTest
import Kafka.Integration.RuntimeContractTest qualified as RuntimeContractTest
import Kafka.MessageTestSupport
import Kafka.Producer qualified as P
import Kafka.TestEnv
import System.Environment (lookupEnv)
import System.Exit (die)
import System.Timeout (timeout)
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertBool, assertFailure, testCase, (@?=))

tests :: TestEnv -> TestTree
tests testEnv =
  testGroup
    "public-contract"
    [ PartitionListContractTest.tests testEnv,
      testGroup "messages" $
        [ testCase "batch conversion releases every returned native message" $
            withRecords testEnv "batch-success" CallbackPollModeAsync $ \_ consumer -> withMessageFault 0 $ do
              records <- pollMessageBatch consumer (Timeout 1000) (BatchSize 3)
              map (fmap crValue) records @?= map (Right . Just) ["first", "second", "third"]
              balanced 3,
          testCase "native message counter detects a retained final object" $
            withRecords testEnv "batch-control" CallbackPollModeAsync $ \_ consumer -> withMessageFault 5 $ do
              records <- pollMessageBatch consumer (Timeout 1000) (BatchSize 3)
              length records @?= 3
              messageStat 1 >>= (@?= 3)
              messageStat 2 >>= (@?= 2)
              messageStat 7 >>= (@?= 1),
          testCase "batch decoder exception releases unprocessed messages" $
            withRecords testEnv "batch-decode" CallbackPollModeAsync $ \_ consumer -> withMessageFault 1 $ do
              result <- try @SomeException (pollMessageBatch consumer (Timeout 1000) (BatchSize 3))
              assertBool (show result) (isLeft result)
              balanced 3,
          testCase "batch native header errors remain errors while later records are released" $
            withRecords testEnv "batch-header" CallbackPollModeAsync $ \_ consumer -> withMessageFault 6 $ do
              records <- pollMessageBatch consumer (Timeout 1000) (BatchSize 3)
              map (fmap crValue) records @?= [Right (Just "first"), Left (KafkaResponseError RdKafkaRespErrBadMsg), Right (Just "third")]
              balanced 3
        ]
          <> [ testCase ("batch cancellation at " <> label <> " releases every acquired message") $
                 withRecords testEnv ("batch-cancel-" <> label) CallbackPollModeAsync $ \_ consumer ->
                   withMessageFault stage (cancelled 3 (pollMessageBatch consumer (Timeout 1000) (BatchSize 3)))
             | (label, stage) <- [("native return", 2), ("middle conversion", 3)]
             ]
          <> [ testCase (label <> " single cancellation at " <> stageName <> " releases its native message") $
                 withRecords testEnv (label <> "-single-" <> stageName) mode $ \_ consumer ->
                   withMessageFault (stage + if mode == CallbackPollModeSync then 3 else 0) (cancelled 1 (pollMessage consumer (Timeout 10000)))
             | (label, mode) <- [("async", CallbackPollModeAsync), ("sync", CallbackPollModeSync)],
               (stageName, stage) <- [("native return", 10), ("inside conversion", 11)]
             ]
          <> [ testCase (label <> " header failure preserves the polled record origin") $
                 withRecords testEnv (label <> "-attributed-header") mode $ \topic consumer -> withMessageFault fault $ do
                   result <- pollMessageAttributed consumer (Timeout 10000)
                   result @?= Left (ConsumerMessageError (KafkaResponseError RdKafkaRespErrBadMsg) (Just topic) (Just (PartitionId 0)) (Just (Offset 0)))
                   balanced 1
             | (label, mode, fault) <- [("async", CallbackPollModeAsync, 12), ("sync", CallbackPollModeSync, 15)]
             ]
          <> [ testCase (label <> " plain polling surfaces native header failure") $
                 withRecords testEnv (label <> "-plain-header") mode $ \_ consumer -> withMessageFault fault $ do
                   pollMessage consumer (Timeout 10000) >>= (@?= Left (KafkaResponseError RdKafkaRespErrBadMsg))
                   balanced 1
             | (label, mode, fault) <- [("async", CallbackPollModeAsync, 12), ("sync", CallbackPollModeSync, 15)]
             ]
          <> [ testCase (label <> " attributed error preserves independently absent " <> absent) $
                 withRecords testEnv (label <> "-absent-" <> absent) mode $ \topic consumer -> withMessageFault fault $ do
                   pollMessageAttributed consumer (Timeout 10000)
                     >>= (@?= Left (ConsumerMessageError (KafkaResponseError RdKafkaRespErrBadMsg) (Just topic) partition offset))
                   balanced 1
             | (label, mode) <- [("async", CallbackPollModeAsync), ("sync", CallbackPollModeSync)],
               (absent, fault, partition, offset) <- [("partition-and-offset", 21, Nothing, Nothing), ("offset", 22, Just (PartitionId 0), Nothing), ("partition", 23, Nothing, Just (Offset 0))]
             ]
          <> [ testCase "callback-event polling cancellation releases a returned message" $
                 withRecords testEnv "events-cancel" CallbackPollModeSync $ \_ consumer ->
                   withMessageFault 13 (cancelled 1 (pollConsumerEvents consumer (Just (Timeout 10000)))),
               testCase "callback buffer preserves attributed header failure" $
                 withRecords testEnv "buffer-attributed-header" CallbackPollModeSync $ \topic consumer -> do
                   withMessageFault 15 $ do
                     pollConsumerEvents consumer (Just (Timeout 10000))
                     balanced 1
                   pollMessageAttributed consumer (Timeout 0) >>= (@?= Left (ConsumerMessageError (KafkaResponseError RdKafkaRespErrBadMsg) (Just topic) (Just (PartitionId 0)) (Just (Offset 0))))
                   message <- pollMessage consumer (Timeout 10000) >>= either throwIO pure
                   crOffset message @?= Offset 1,
               testCase "callback buffer preserves plain header failure" $
                 withRecords testEnv "buffer-plain-header" CallbackPollModeSync $ \_ consumer -> do
                   withMessageFault 15 $ do
                     pollConsumerEvents consumer (Just (Timeout 10000))
                     balanced 1
                   pollMessage consumer (Timeout 0) >>= (@?= Left (KafkaResponseError RdKafkaRespErrBadMsg))
                   message <- pollMessage consumer (Timeout 10000) >>= either throwIO pure
                   crOffset message @?= Offset 1,
               testCase "originless callback error survives seek filtering and a paused partition" $
                 withRecords testEnv "buffer-originless-error" CallbackPollModeSync $ \topic consumer -> do
                   withMessageFault 24 $ do
                     pollConsumerEvents consumer (Just (Timeout 10000))
                     balanced 1
                   seekPartitions consumer [TopicPartition topic (PartitionId 0) (PartitionOffset 2)] (Timeout 2000) >>= (@?= Nothing)
                   pausePartitions consumer [(topic, PartitionId 0)] >>= (@?= KafkaResponseError RdKafkaRespErrNoError)
                   pollMessageAttributed consumer (Timeout 0)
                     >>= (@?= Left (ConsumerMessageError (KafkaResponseError RdKafkaRespErrBadMsg) Nothing Nothing Nothing))
                   resumePartitions consumer [(topic, PartitionId 0)] >>= (@?= KafkaResponseError RdKafkaRespErrNoError)
                   record <- pollMessage consumer (Timeout 10000) >>= either throwIO pure
                   (crTopic record, crPartition record, crOffset record, crValue record) @?= (topic, PartitionId 0, Offset 2, Just "third"),
               testCase "callback-event polling releases and discards a returned fatal frame" $
                 withRecords testEnv "buffer-fatal-discard" CallbackPollModeSync $ \topic consumer -> do
                   withMessageFault 25 $ do
                     pollConsumerEvents consumer (Just (Timeout 10000))
                     balanced 1
                   record <- pollMessage consumer (Timeout 10000) >>= either throwIO pure
                   (crTopic record, crOffset record, crValue record) @?= (topic, Offset 1, Just "second"),
               testCase "zero batch size leaves the queued records available" $
                 withRecords testEnv "batch-zero" CallbackPollModeAsync $ \_ consumer -> withMessageFault 0 $ do
                   pollMessageBatch consumer (Timeout 1000) (BatchSize 0) >>= (@?= [])
                   records <- pollMessageBatch consumer (Timeout 1000) (BatchSize 3)
                   map (fmap crValue) records @?= map (Right . Just) ["first", "second", "third"]
                   balanced 3,
               testCase "unsupported sync batch does not discard a record" $
                 withRecords testEnv "batch-sync" CallbackPollModeSync $ \_ consumer -> withMessageFault 20 $ do
                   pollMessageBatch consumer (Timeout 0) (BatchSize 3) >>= (@?= [Left (KafkaBadSpecification "Calling pollMessageBatch while CallbackPollMode is set to CallbackPollModeSync.")])
                   messageStat 0 >>= (@?= 0)
                   record <- pollMessage consumer (Timeout 10000) >>= either throwIO pure
                   (crOffset record, crValue record) @?= (Offset 0, Just "first"),
               testCase "signed native batch failure preserves its error and queued data" $
                 withRecords testEnv "batch-native-error" CallbackPollModeAsync $ \_ consumer -> do
                   withMessageFault 4 $ do
                     pollMessageBatch consumer (Timeout 1000) (BatchSize 3) >>= (@?= [Left (KafkaResponseError RdKafkaRespErrTimedOut)])
                     balanced 0
                   withMessageFault 0 $ do
                     records <- pollMessageBatch consumer (Timeout 1000) (BatchSize 3)
                     map (fmap crValue) records @?= map (Right . Just) ["first", "second", "third"]
                     balanced 3,
               testCase "infinite batch timeout returns available records" $
                 withRecords testEnv "batch-infinite" CallbackPollModeAsync $ \_ consumer -> withMessageFault 0 $ do
                   records <- pollMessageBatch consumer (Timeout (-1)) (BatchSize 3)
                   map (fmap crValue) records @?= map (Right . Just) ["first", "second", "third"]
                   balanced 3
             ]
          <> [ testCase ("batch capacity " <> show capacity <> " preserves complete ordered records across partial batches") $
                 withRecords testEnv ("batch-fields-" <> show capacity) CallbackPollModeAsync $ \topic consumer -> withMessageFault 20 $ do
                   let collect records
                         | length records == 3 = pure records
                         | otherwise = do
                             batch <- pollMessageBatch consumer (Timeout 500) (BatchSize capacity)
                             assertBool (show batch) (length batch <= capacity)
                             next <- traverse (either throwIO pure) batch
                             assertBool (show (records <> next)) (length records + length next <= 3)
                             collect (records <> next)
                   records <- bounded (collect [])
                   forM_ (zip3 records [0 ..] ["first", "second", "third"]) $ \(record, offset, value) -> do
                     (crTopic record, crPartition record, crOffset record, crKey record, crValue record, crHeaders record)
                       @?= (topic, PartitionId 0, Offset offset, Nothing, Just value, mempty)
                     case crTimestamp record of
                       CreateTime (Millis time) -> assertBool "missing batch timestamp" (time > 0)
                       other -> assertFailure (show other)
                   pollMessageBatch consumer (Timeout 0) (BatchSize capacity) >>= (@?= [])
                   balanced 3
             | capacity <- [1, 2, 5]
             ]
          <> [ testCase ("invalid batch array size is rejected before native acquisition: " <> show size) $
                 withRecords testEnv ("batch-size-" <> show size) CallbackPollModeAsync $ \_ consumer -> withMessageFault 20 $ do
                   pollMessageBatch consumer (Timeout 0) (BatchSize size) >>= (@?= [Left (KafkaBadSpecification "Batch size must be nonnegative and fit a native pointer array.")])
                   messageStat 0 >>= (@?= 0)
                   balanced 0
             | size <- [-1, minBound, maxBound]
             ]
          <> [ testCase "batch preserves complete records followed by exact partition EOF and idle timeout" $
                 withTopic testEnv "batch-eof-fields" 1 $ \topic producer -> do
                   forM_ ["first", "second", "third"] (produceTo producer topic 0)
                   let props = brokersList (teBrokers testEnv) <> groupId (makeGroupId testEnv "batch-eof-fields") <> noAutoCommit <> noAutoOffsetStore <> callbackPollMode CallbackPollModeAsync <> extraProp "enable.partition.eof" "true"
                   bracket (newAssignedConsumer props >>= either throwIO pure) releaseConsumer $ \consumer -> do
                     assign consumer [TopicPartition topic (PartitionId 0) PartitionOffsetBeginning] >>= (@?= Nothing)
                     withMessageFault 0 $ do
                       let eof = KafkaResponseError RdKafkaRespErrPartitionEof
                           collect values
                             | any (\case Left err -> err == eof; _ -> False) values = pure values
                             | otherwise = do
                                 batch <- pollMessageBatch consumer (Timeout 500) (BatchSize 5)
                                 assertBool (show batch) (length batch <= 5)
                                 collect (values <> batch)
                       results <- bounded (collect [])
                       map (fmap (\record -> (crTopic record, crPartition record, crOffset record, crKey record, crValue record, crHeaders record))) results
                         @?= [Right (topic, PartitionId 0, Offset offset, Nothing, Just value, mempty) | (offset, value) <- zip [0 ..] ["first", "second", "third"]] <> [Left eof]
                       forM_ results $ \case
                         Right record -> case crTimestamp record of CreateTime (Millis time) -> assertBool "missing EOF batch record timestamp" (time > 0); other -> assertFailure (show other)
                         Left _ -> pure ()
                       pollMessageBatch consumer (Timeout 100) (BatchSize 5) >>= (@?= [])
                       balanced 4
             ]
          <> [ testCase ("empty batch respects timeout " <> show milliseconds)
                 $ bracket
                   (newAssignedConsumer (brokersList (teBrokers testEnv) <> groupId (makeGroupId testEnv (Text.pack ("batch-empty-" <> show milliseconds))) <> noAutoCommit) >>= either throwIO pure)
                   releaseConsumer
                 $ \consumer -> withMessageFault 20 $ do
                   start <- getMonotonicTimeNSec
                   pollMessageBatch consumer (Timeout milliseconds) (BatchSize 3) >>= (@?= [])
                   elapsed <- subtract start <$> getMonotonicTimeNSec
                   assertBool ("empty batch took " <> show elapsed <> " ns") (elapsed < 3000000000)
                   balanced 0
             | milliseconds <- [0, 100]
             ],
      rebalanceErrorTests testEnv,
      ConsumerStateContractTest.tests testEnv,
      ConsumerCommitContractTest.tests testEnv,
      DiagnosticContractTest.tests testEnv,
      InputBoundaryContractTest.tests testEnv,
      DeprecatedRunnerContractTest.tests testEnv,
      FatalErrorTest.tests testEnv,
      QueueEventTest.tests testEnv,
      RuntimeContractTest.tests testEnv,
      MetadataContractTest.tests testEnv,
      ConfigStringContractTest.tests testEnv
    ]

withRecords :: TestEnv -> String -> CallbackPollMode -> (TopicName -> KafkaConsumer -> IO a) -> IO a
withRecords testEnv label mode action = withTopic testEnv ("message-owner-" <> safeLabel) 1 $ \topic producer -> do
  sendMessages [P.ProducerRecord topic (P.SpecifiedPartition 0) Nothing (Just value) mempty | value <- ["first", "second", "third"]] producer >>= either throwIO pure
  let props = brokersList (teBrokers testEnv) <> groupId (makeGroupId testEnv ("message-owner-" <> safeLabel)) <> noAutoCommit <> callbackPollMode mode
  bracket (newAssignedConsumer props >>= either throwIO pure) releaseConsumer $ \consumer -> do
    assign consumer [TopicPartition topic (PartitionId 0) PartitionOffsetBeginning] >>= (@?= Nothing)
    action topic consumer
  where
    safeLabel = Text.replace " " "-" (Text.pack label)

balanced :: Int -> IO ()
balanced count = do
  messageStat 1 >>= (@?= count)
  messageStat 2 >>= (@?= count)
  messageStat 5 >>= (@?= 0)

bounded :: IO a -> IO a
bounded action = timeout 10000000 action >>= maybe (assertFailure "message worker timed out") pure

cancelled :: Int -> IO a -> IO ()
cancelled count action = do
  done <- newEmptyMVar
  worker <- forkIOWithUnmask $ \unmask -> try @SomeException (unmask action) >>= putMVar done
  flip finally (releaseMessageBoundary >> bounded (readMVar done)) $ do
    awaitMessageBoundary
    messageStat 1 >>= (@?= count)
    killed <- newEmptyMVar
    killer <- forkFinally (killThread worker) (putMVar killed)
    let awaitPending =
          threadStatus killer >>= \status ->
            if status == ThreadBlocked BlockedOnException then pure () else threadDelay 1000 >> awaitPending
    bounded awaitPending
    tryReadMVar done >>= maybe (pure ()) (const (assertFailure "poll returned while the native boundary was held"))
    releaseMessageBoundary
    bounded (readMVar done) >>= \case
      Left exception -> (fromException exception :: Maybe AsyncException) @?= Just ThreadKilled
      Right _ -> assertFailure "pending polling cancellation was not delivered"
    bounded (readMVar killed) >>= either throwIO pure
    balanced count

rebalanceErrorTests :: TestEnv -> TestTree
rebalanceErrorTests testEnv =
  testGroup "rebalance-errors" $
    [ testCase
        "native rebalance counter detects an undisposed error"
        ( withStateFault 1 $ do
            stateLeakControl
            stateStat 0 >>= (@?= 1)
            stateStat 1 >>= (@?= 0)
        )
    ]
      <> [ testCase (label <> " real cooperative assignment and revocation preserve records") $
             withTopic testEnv ("cooperative-" <> Text.pack label) 1 $ \topic producer -> do
               produceTo producer topic 0 "cooperative-record"
               events <- newIORef []
               observedAssignments <- newIORef []
               let partitions = [(topic, PartitionId 0)]
                   props =
                     brokersList (teBrokers testEnv)
                       <> groupId (makeGroupId testEnv ("cooperative-" <> Text.pack label))
                       <> noAutoCommit
                       <> callbackPollMode mode
                       <> extraProps (M.singleton "partition.assignment.strategy" "cooperative-sticky")
                       <> setCallback
                         ( rebalanceCallback
                             ( \consumer event -> do
                                 modifyIORef' events (<> [event])
                                 current <- assignment consumer
                                 modifyIORef' observedAssignments (<> [current])
                             )
                         )
               bracket (newConsumer props (topics [topic] <> offsetReset Earliest) >>= either throwIO pure) releaseConsumer $ \consumer -> do
                 record <- pollMessage consumer (Timeout 10000) >>= either throwIO pure
                 (crOffset record, crValue record) @?= (Offset 0, Just "cooperative-record")
                 readIORef events >>= (@?= [RebalanceBeforeAssign partitions, RebalanceAssign partitions])
               readIORef events >>= (@?= [RebalanceBeforeAssign partitions, RebalanceAssign partitions, RebalanceBeforeRevoke partitions, RebalanceRevoke partitions])
               readIORef observedAssignments >>= (@?= map Right [M.empty, M.singleton topic [PartitionId 0], M.singleton topic [PartitionId 0], M.empty])
         | (label, mode) <- [("async", CallbackPollModeAsync), ("sync", CallbackPollModeSync)]
         ]
      <> [ testCase (protocol <> " " <> operation <> " " <> poll <> " " <> failure <> " reports failure without a success event") $
             runChildProbe ("rebalance-" <> protocol <> "-" <> operation <> "-" <> poll <> "-" <> failure) ["--rebalance-error-probe", protocol, operation, poll, failure]
         | protocol <- ["cooperative", "eager"],
           operation <- ["assign", "revoke"],
           poll <- ["async", "sync"],
           failure <- if protocol == "cooperative" then ["native", "decode"] else ["native"]
         ]
      <> [ testCase (poll <> " unexpected rebalance error " <> description) $
             runChildProbe ("rebalance-unexpected-" <> poll <> "-" <> failure) ["--rebalance-error-probe", "unexpected", "failure", poll, failure]
         | poll <- ["async", "sync"],
           (failure, description) <- [("native", "clears assignment and pending records"), ("reset-failure", "preserves state when native reset fails")]
         ]

runRebalanceProbe :: String -> String -> String -> String -> IO ()
runRebalanceProbe "unexpected" _ poll failure = runUnexpectedRebalanceProbe poll failure
runRebalanceProbe protocol operation poll failure = do
  fault <- case (protocol, operation, failure) of
    ("cooperative", "assign", "native") -> pure 1
    ("cooperative", "revoke", "native") -> pure 2
    ("cooperative", "assign", "decode") -> pure 3
    ("cooperative", "revoke", "decode") -> pure 4
    ("eager", "assign", "native") -> pure 5
    ("eager", "revoke", "native") -> pure 6
    _ -> die "invalid rebalance error probe"
  brokers <- lookupEnv "RDKAFKA_TEST_BROKER" >>= either die pure . resolveBrokers
  testEnv <- newTestEnv brokers
  events <- newIORef []
  reported <- newEmptyMVar
  let topic = TopicName (prefixedName testEnv "rebalance-error")
      mode = if poll == "async" then CallbackPollModeAsync else CallbackPollModeSync
      props =
        brokersList brokers
          <> groupId (makeGroupId testEnv "rebalance-error")
          <> noAutoCommit
          <> callbackPollMode mode
          <> setCallback (rebalanceCallback (\_ event -> modifyIORef' events (<> [event])))
      before = if operation == "assign" then RebalanceBeforeAssign [(topic, PartitionId 0)] else RebalanceBeforeRevoke [(topic, PartitionId 0)]
  bracket getUncaughtExceptionHandler setUncaughtExceptionHandler $ \_ -> do
    setUncaughtExceptionHandler (putMVar reported)
    withStateFault fault
      $ bracket
        (newAssignedConsumer props >>= either throwIO pure)
        (\consumer -> endStateFault `finally` destroyConsumer consumer)
      $ \_ -> do
        triggerRebalance topic
        result <- timeout 1000000 (readMVar reported)
        observed <- mapM stateStat [0 .. 6]
        seen <- readIORef events
        let expectedObject = if protocol == "cooperative" then 1 else 0
            expected = [expectedObject, expectedObject, if protocol == "cooperative" && operation == "assign" then 1 else 0, if protocol == "cooperative" && operation == "revoke" then 1 else 0, 1, if protocol == "eager" then 1 else 0, 1]
            expectedError = case result of
              Nothing -> False
              Just exception ->
                if failure == "decode"
                  then isJust (fromException exception :: Maybe ErrorCall)
                  else (fromException exception :: Maybe KafkaError) == Just (KafkaResponseError RdKafkaRespErrState)
        assertBool (show (observed, seen, fmap show result)) (observed == expected && seen == [before] && expectedError)

runUnexpectedRebalanceProbe :: String -> String -> IO ()
runUnexpectedRebalanceProbe poll failure = do
  brokers <- lookupEnv "RDKAFKA_TEST_BROKER" >>= either die pure . resolveBrokers
  testEnv <- newTestEnv brokers
  events <- newIORef []
  reported <- newEmptyMVar
  let mode = if poll == "async" then CallbackPollModeAsync else CallbackPollModeSync
      fault = if failure == "native" then 7 else 8
      expected = KafkaResponseError (if failure == "native" then RdKafkaRespErrGroupAuthorizationFailed else RdKafkaRespErrState)
      props = brokersList brokers <> groupId (makeGroupId testEnv "unexpected-rebalance") <> noAutoCommit <> noAutoOffsetStore <> callbackPollMode mode <> setCallback (rebalanceCallback (\_ event -> modifyIORef' events (<> [event])))
  bracket getUncaughtExceptionHandler setUncaughtExceptionHandler $ \_ -> do
    setUncaughtExceptionHandler (putMVar reported)
    withTopic testEnv (Text.pack ("unexpected-rebalance-" <> poll <> failure)) 1 $ \topic producer ->
      withStateFault fault $
        bracket (newAssignedConsumer props >>= either throwIO pure) (\consumer -> endStateFault `finally` releaseConsumer consumer) $ \consumer -> do
          assign consumer [TopicPartition topic (PartitionId 0) PartitionOffsetBeginning] >>= (@?= Nothing)
          produceTo producer topic 0 "pending-before-rebalance-error"
          if mode == CallbackPollModeSync then pollConsumerEvents consumer (Just (Timeout 10000)) else pure ()
          beforeCalls <- stateStat 5
          triggerRebalance topic
          result <- timeout 1000000 (readMVar reported)
          fmap (\exception -> fromException exception :: Maybe KafkaError) result @?= Just (Just expected)
          stateStat 5 >>= (@?= beforeCalls + 1)
          readIORef events >>= (@?= [])
          if failure == "native"
            then do
              assignment consumer >>= (@?= Right M.empty)
              pollMessage consumer (Timeout 0) >>= (@?= Left (KafkaResponseError RdKafkaRespErrTimedOut))
            else do
              assignment consumer >>= (@?= Right (M.singleton topic [PartitionId 0]))
              record <- pollMessage consumer (Timeout 10000) >>= either throwIO pure
              (crTopic record, crPartition record, crOffset record, crValue record) @?= (topic, PartitionId 0, Offset 0, Just "pending-before-rebalance-error")
