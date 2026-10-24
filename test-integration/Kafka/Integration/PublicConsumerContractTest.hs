module Kafka.Integration.PublicConsumerContractTest (tests, runRebalanceProbe) where

import Control.Concurrent (forkFinally, forkIOWithUnmask, killThread, threadDelay)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, readMVar, tryReadMVar)
import Control.Exception (AsyncException (ThreadKilled), ErrorCall, SomeException, bracket, finally, fromException, throwIO, try)
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
import Kafka.Integration.ConsumerStateContractTest qualified as ConsumerStateContractTest
import Kafka.Integration.DiagnosticContractTest qualified as DiagnosticContractTest
import Kafka.Integration.MetadataContractTest qualified as MetadataContractTest
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
    [ testGroup "messages" $
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
          <> [ testCase "callback-event polling cancellation releases a returned message" $
                 withRecords testEnv "events-cancel" CallbackPollModeSync $ \_ consumer ->
                   withMessageFault 13 (cancelled 1 (pollConsumerEvents consumer (Just (Timeout 10000)))),
               testCase "zero batch size leaves the queued records available" $
                 withRecords testEnv "batch-zero" CallbackPollModeAsync $ \_ consumer -> withMessageFault 0 $ do
                   pollMessageBatch consumer (Timeout 1000) (BatchSize 0) >>= (@?= [])
                   records <- pollMessageBatch consumer (Timeout 1000) (BatchSize 3)
                   map (fmap crValue) records @?= map (Right . Just) ["first", "second", "third"]
                   balanced 3,
               testCase "unsupported sync batch does not discard a record" $
                 withRecords testEnv "batch-sync" CallbackPollModeSync $ \_ consumer -> withMessageFault 20 $ do
                   pollMessageBatch consumer (Timeout 0) (BatchSize 3) >>= assertBadSpecification
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
          <> [ testCase ("invalid batch array size is rejected before native acquisition: " <> show size) $
                 withRecords testEnv ("batch-size-" <> show size) CallbackPollModeAsync $ \_ consumer -> withMessageFault 20 $ do
                   pollMessageBatch consumer (Timeout 0) (BatchSize size) >>= assertBadSpecification
                   messageStat 0 >>= (@?= 0)
                   balanced 0
             | size <- [-1, minBound, maxBound]
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
      DiagnosticContractTest.tests testEnv,
      RuntimeContractTest.tests testEnv,
      MetadataContractTest.tests testEnv
    ]

assertBadSpecification :: (Show a) => [Either KafkaError a] -> IO ()
assertBadSpecification [Left (KafkaBadSpecification _)] = pure ()
assertBadSpecification result = assertFailure (show result)

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
               let partitions = [(topic, PartitionId 0)]
                   props =
                     brokersList (teBrokers testEnv)
                       <> groupId (makeGroupId testEnv ("cooperative-" <> Text.pack label))
                       <> noAutoCommit
                       <> callbackPollMode mode
                       <> extraProps (M.singleton "partition.assignment.strategy" "cooperative-sticky")
                       <> setCallback (rebalanceCallback (\_ event -> modifyIORef' events (<> [event])))
               bracket (newConsumer props (topics [topic] <> offsetReset Earliest) >>= either throwIO pure) releaseConsumer $ \consumer -> do
                 record <- pollMessage consumer (Timeout 10000) >>= either throwIO pure
                 (crOffset record, crValue record) @?= (Offset 0, Just "cooperative-record")
                 readIORef events >>= (@?= [RebalanceBeforeAssign partitions, RebalanceAssign partitions])
               readIORef events >>= (@?= [RebalanceBeforeAssign partitions, RebalanceAssign partitions, RebalanceBeforeRevoke partitions, RebalanceRevoke partitions])
         | (label, mode) <- [("async", CallbackPollModeAsync), ("sync", CallbackPollModeSync)]
         ]
      <> [ testCase (protocol <> " " <> operation <> " " <> poll <> " " <> failure <> " reports failure without a success event") $
             runChildProbe ("rebalance-" <> protocol <> "-" <> operation <> "-" <> poll <> "-" <> failure) ["--rebalance-error-probe", protocol, operation, poll, failure]
         | protocol <- ["cooperative", "eager"],
           operation <- ["assign", "revoke"],
           poll <- ["async", "sync"],
           failure <- if protocol == "cooperative" then ["native", "decode"] else ["native"]
         ]

runRebalanceProbe :: String -> String -> String -> String -> IO ()
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
