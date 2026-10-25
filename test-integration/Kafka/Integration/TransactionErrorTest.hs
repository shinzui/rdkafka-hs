module Kafka.Integration.TransactionErrorTest (tests) where

import Control.Concurrent (forkFinally, forkIOWithUnmask, killThread, threadDelay)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, readMVar, tryReadMVar)
import Control.Exception (AsyncException (ThreadKilled), SomeException, allowInterrupt, bracket, bracket_, finally, fromException, throwIO, try)
import Control.Monad (forM_, replicateM_, void, when)
import Data.ByteString qualified as BS
import Data.Either (isLeft)
import Data.Int (Int64)
import Data.Map.Strict qualified as M
import Data.Text qualified as Text
import GHC.Conc (BlockReason (BlockedOnException), ThreadStatus (ThreadBlocked), threadStatus)
import Kafka.Consumer qualified as C
import Kafka.Producer qualified as P
import Kafka.TestEnv hiding (withProducer)
import Kafka.Topic qualified as Topic
import Kafka.Transaction qualified as T
import Kafka.TransactionTestSupport
import System.Timeout (timeout)
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertBool, assertFailure, testCase, (@?=))

tests :: TestEnv -> TestTree
tests testEnv =
  testGroup "transaction-errors" $
    [ testCase "transaction-error-leak-probe releases 300 immediate native errors" $
        withClients testEnv "leaks" $ \producer consumer -> withTxFault 0 $ do
          replicateM_ 100 $ forM_ ["init", "begin", "abort"] $ \operation ->
            runOperation operation producer consumer >>= (@?= notConfigured)
          txStat 0 >>= (@?= 300)
          balanced 300,
      testCase "native allocation control detects the inherited three-error leak" $
        withClients testEnv "control" $ \producer _ -> withTxFault 0 $ do
          T.beginTransaction producer >>= (@?= notConfigured)
          balanced 1
          nativeLeakControl
          txStat 0 >>= (@?= 4)
          txStat 1 >>= (@?= 1),
      testCase "initialization timeout destroys its native error on each retry" $
        withProducer testEnv "timeout" True ["127.0.0.1:1"] $ \producer -> withTxFault 0 $ do
          replicateM_ 3 $ T.initTransactions producer (C.Timeout 10) >>= (@?= Just (C.KafkaResponseError C.RdKafkaRespErrTimedOut))
          txStat 0 >>= (@?= 3)
          balanced 3,
      testCase "zero-timeout initialization resumes successfully against the broker" $
        withProducer testEnv "resume" True (teBrokers testEnv) $ \producer -> withTxFault 0 $ do
          T.initTransactions producer (C.Timeout 0) >>= (@?= Just (C.KafkaResponseError C.RdKafkaRespErrTimedOut))
          T.initTransactions producer (C.Timeout 10000) >>= (@?= Nothing)
          T.beginTransaction producer >>= (@?= Nothing)
          T.commitTransaction producer (C.Timeout 10000) >>= noTxError
          balanced 1,
      testCase "transaction state errors preserve empty commit and abort recovery" $
        withProducer testEnv "states" True (teBrokers testEnv) $ \producer -> withTxFault 0 $ do
          T.beginTransaction producer >>= (@?= stateError)
          T.initTransactions producer (C.Timeout 10000) >>= (@?= Nothing)
          T.beginTransaction producer >>= (@?= Nothing)
          T.beginTransaction producer >>= (@?= stateError)
          T.abortTransaction producer (C.Timeout 10000) >>= (@?= Nothing)
          T.abortTransaction producer (C.Timeout 1000) >>= (@?= stateError)
          T.commitTransaction producer (C.Timeout 1000) >>= checkTx C.RdKafkaRespErrState (False, False, False)
          T.beginTransaction producer >>= (@?= Nothing)
          T.commitTransaction producer (C.Timeout 10000) >>= noTxError
          T.initTransactions producer (C.Timeout 1000) >>= (@?= stateError)
          txStat 0 >>= (@?= 5)
          balanced 5,
      testCase "duplicate transactional IDs report a real fatal fencing error" $ fencing testEnv,
      testCase "broker-rejected delivery requires abort and permits a successful new transaction" $ abortRecovery testEnv
    ]
      <> [ testCase (operation <> " without transactional ID preserves native code and destroys error") $
             withClients testEnv operation $ \producer consumer -> withTxFault 0 $ do
               runOperation operation producer consumer >>= (@?= notConfigured)
               balanced 1
               if operation == "offset" then metadataBalanced else pure ()
         | operation <- operations
         ]
      <> [ testCase (operation <> " NULL return remains success") $
             withClients testEnv ("null-" <> operation) $ \producer consumer -> withTxFault 201 $ do
               runOperation operation producer consumer >>= (@?= Nothing)
               txStat 0 >>= (@?= 0)
               balanced 0
         | operation <- operations
         ]
      <> [ testCase (operation <> " invalid native code still destroys acquired error") $
             withClients testEnv ("decode-" <> operation) $ \producer consumer -> withTxFault 200 $ do
               result <- try @SomeException (runOperation operation producer consumer)
               assertBool (show result) (isLeft result)
               balanced 1
               if operation == "offset" then metadataBalanced else pure ()
         | operation <- operations
         ]
      <> [ testCase (operation <> " copies native flag combination " <> show bits <> " before destruction") $ do
             when (bits /= 0) requireNativeFlagDiagnostics
             withClients testEnv ("flags-" <> operation <> show bits) $ \producer consumer -> withTxFault (100 + bits) $ do
               result <- if operation == "commit" then T.commitTransaction producer (C.Timeout 1000) else T.commitOffsetMessageTransaction producer consumer dummy (C.Timeout 1000)
               checkTx C.RdKafkaRespErrState (odd bits, odd (bits `div` 2), odd (bits `div` 4)) result
               balanced 1
               forM_ [2 .. 5] $ \index -> txStat index >>= (@?= 1)
               if operation == "offset" then metadataBalanced >> (txStat 12 >>= (@?= 8)) else pure ()
         | operation <- ["commit", "offset"],
           bits <- [0 .. 7]
         ]
      <> [ testCase (operation <> " cancellation at decoder stage " <> show stage <> " releases once") $
             withClients testEnv ("cancel-" <> operation <> show stage) $ \producer consumer ->
               withTxFault stage (cancelDecoder operation producer consumer)
         | operation <- operations,
           stage <- if operation `elem` ["commit", "offset"] then [20 .. 23] else [20]
         ]
      <> [ testCase (label <> " commit publishes output and source next offset atomically; abort publishes neither") $
             atomicOffsets testEnv label mode
         | (label, mode) <- [("async", C.CallbackPollModeAsync), ("sync", C.CallbackPollModeSync)]
         ]

operations :: [String]
operations = ["init", "begin", "abort", "commit", "offset"]

notConfigured, stateError :: Maybe C.KafkaError
notConfigured = Just (C.KafkaResponseError C.RdKafkaRespErrNotConfigured)
stateError = Just (C.KafkaResponseError C.RdKafkaRespErrState)

dummy :: C.ConsumerRecord (Maybe BS.ByteString) (Maybe BS.ByteString)
dummy = C.ConsumerRecord (C.TopicName "transaction-source-fixture") (C.PartitionId 0) (C.Offset 7) C.NoTimestamp mempty Nothing Nothing

runOperation :: String -> P.KafkaProducer -> C.KafkaConsumer -> IO (Maybe C.KafkaError)
runOperation operation producer consumer = case operation of
  "init" -> T.initTransactions producer (C.Timeout 1000)
  "begin" -> T.beginTransaction producer
  "abort" -> T.abortTransaction producer (C.Timeout 1000)
  "commit" -> fmap T.getKafkaError <$> T.commitTransaction producer (C.Timeout 1000)
  _ -> fmap T.getKafkaError <$> T.commitOffsetMessageTransaction producer consumer dummy (C.Timeout 1000)

withProducer :: TestEnv -> String -> Bool -> [C.BrokerAddress] -> (P.KafkaProducer -> IO a) -> IO a
withProducer testEnv label transactional brokers = bracket acquire cleanup
  where
    cleanup producer =
      when transactional (void (T.abortTransaction producer (C.Timeout 5000))) `finally` releaseProducer producer
    acquire =
      P.newProducer
        ( P.brokersList brokers
            <> P.extraProp "log_level" "0"
            <> if transactional then P.extraProp "transactional.id" (prefixedName testEnv (Text.pack ("tx-" <> label))) else mempty
        )
        >>= either throwIO pure

withClients :: TestEnv -> String -> (P.KafkaProducer -> C.KafkaConsumer -> IO a) -> IO a
withClients testEnv label action = withProducer testEnv label False (teBrokers testEnv) $ \producer ->
  bracket
    (C.newAssignedConsumer (C.brokersList (teBrokers testEnv) <> C.groupId (makeGroupId testEnv (Text.pack ("tx-" <> label))) <> C.noAutoCommit) >>= either throwIO pure)
    releaseConsumer
    (action producer)

balanced :: Int64 -> IO ()
balanced count = do
  txStat 0 >>= (@?= count)
  txStat 1 >>= (@?= count)
  txStat 8 >>= (@?= 0)

metadataBalanced :: IO ()
metadataBalanced = do
  txStat 6 >>= (@?= 1)
  txStat 7 >>= (@?= 1)

checkTx :: C.RdKafkaRespErrT -> (Bool, Bool, Bool) -> Maybe T.TxError -> IO ()
checkTx code flags = \case
  Nothing -> assertFailure "expected a native transaction error"
  Just result -> do
    T.getKafkaError result @?= C.KafkaResponseError code
    (T.kafkaErrorIsFatal result, T.kafkaErrorIsRetriable result, T.kafkaErrorTxnRequiresAbort result) @?= flags

noTxError :: Maybe T.TxError -> IO ()
noTxError = maybe (pure ()) (assertFailure . show . T.getKafkaError)

bounded :: IO a -> IO a
bounded action = timeout 10000000 action >>= maybe (assertFailure "transaction stage timed out") pure

cancelDecoder :: String -> P.KafkaProducer -> C.KafkaConsumer -> IO ()
cancelDecoder operation producer consumer = do
  done <- newEmptyMVar
  worker <- forkIOWithUnmask $ \unmask -> try @SomeException (unmask (runOperation operation producer consumer <* allowInterrupt)) >>= putMVar done
  flip finally (releaseTxBoundary >> bounded (readMVar done)) $ do
    awaitTxBoundary
    txStat 0 >>= (@?= 1)
    txStat 1 >>= (@?= 0)
    killed <- newEmptyMVar
    killer <- forkFinally (killThread worker) (putMVar killed)
    let awaitPending =
          threadStatus killer >>= \status ->
            if status == ThreadBlocked BlockedOnException then pure () else threadDelay 1000 >> awaitPending
    bounded awaitPending
    tryReadMVar done >>= maybe (pure ()) (const (assertFailure "decoder returned before observed cancellation"))
    releaseTxBoundary
    bounded (readMVar done) >>= \case
      Left exception -> (fromException exception :: Maybe AsyncException) @?= Just ThreadKilled
      Right _ -> assertFailure "pending decoder cancellation was not delivered"
    bounded (readMVar killed) >>= either throwIO pure
    balanced 1
    if operation == "offset" then metadataBalanced else pure ()

atomicOffsets :: TestEnv -> String -> C.CallbackPollMode -> IO ()
atomicOffsets testEnv label mode =
  withTopic testEnv (Text.pack ("tx-source-" <> label)) 1 $ \source ordinary ->
    withTopic testEnv (Text.pack ("tx-output-" <> label)) 1 $ \output _ ->
      withProducer testEnv ("atomic-" <> label) True (teBrokers testEnv) $ \producer -> do
        produceTo ordinary source 0 "first-input"
        produceTo ordinary source 0 "second-input"
        let props = C.brokersList (teBrokers testEnv) <> C.groupId (makeGroupId testEnv (Text.pack ("tx-source-" <> label))) <> C.noAutoCommit <> C.noAutoOffsetStore <> C.callbackPollMode mode
        bracket (C.newConsumer props (C.topics [source] <> C.offsetReset C.Earliest) >>= either throwIO pure) releaseConsumer $ \consumer -> do
          first <- C.pollMessage consumer (C.Timeout 10000) >>= either throwIO pure
          C.crOffset first @?= C.Offset 0
          committedOffset consumer source >>= (@?= C.PartitionOffsetInvalid)
          T.initTransactions producer (C.Timeout 10000) >>= (@?= Nothing)
          T.beginTransaction producer >>= (@?= Nothing)
          produceTo producer output 0 "aborted-output"
          T.commitOffsetMessageTransaction producer consumer first (C.Timeout 10000) >>= noTxError
          T.abortTransaction producer (C.Timeout 10000) >>= (@?= Nothing)
          committedOffset consumer source >>= (@?= C.PartitionOffsetInvalid)
          T.beginTransaction producer >>= (@?= Nothing)
          produceTo producer output 0 "committed-output"
          T.commitOffsetMessageTransaction producer consumer first (C.Timeout 10000) >>= noTxError
          T.commitTransaction producer (C.Timeout 10000) >>= noTxError
          committedOffset consumer source >>= (@?= C.PartitionOffset 1)
          second <- C.pollMessage consumer (C.Timeout 10000) >>= either throwIO pure
          C.crOffset second @?= C.Offset 1
          T.beginTransaction producer >>= (@?= Nothing)
          produceTo producer output 0 "next-output"
          T.commitOffsetMessageTransaction producer consumer second (C.Timeout 10000) >>= noTxError
          T.commitTransaction producer (C.Timeout 10000) >>= noTxError
          committedOffset consumer source >>= (@?= C.PartitionOffset 2)
        visible testEnv mode (label <> "-committed") output "read_committed" ["committed-output", "next-output"]
        visible testEnv mode (label <> "-uncommitted") output "read_uncommitted" ["aborted-output", "committed-output", "next-output"]

committedOffset :: C.KafkaConsumer -> C.TopicName -> IO C.PartitionOffset
committedOffset consumer topic =
  C.committed consumer (C.Timeout 10000) [(topic, C.PartitionId 0)] >>= \case
    Right [C.TopicPartition actual (C.PartitionId 0) offset] | actual == topic -> pure offset
    other -> assertFailure (show other)

visible :: TestEnv -> C.CallbackPollMode -> String -> C.TopicName -> Text.Text -> [BS.ByteString] -> IO ()
visible testEnv mode label topic level expected = bracket acquire releaseConsumer $ \consumer -> do
  C.assign consumer [C.TopicPartition topic (C.PartitionId 0) C.PartitionOffsetBeginning] >>= (@?= Nothing)
  records <- receiveMessages consumer (length expected) >>= either throwIO pure
  map C.crValue records @?= map Just expected
  where
    acquire =
      C.newAssignedConsumer
        (C.brokersList (teBrokers testEnv) <> C.groupId (makeGroupId testEnv (Text.pack ("tx-visible-" <> label))) <> C.noAutoCommit <> C.callbackPollMode mode <> C.extraProps (M.singleton "isolation.level" level))
        >>= either throwIO pure

fencing :: TestEnv -> IO ()
fencing testEnv = withTopic testEnv "tx-fencing" 1 $ \topic _ ->
  withProducer testEnv "fenced" True (teBrokers testEnv) $ \first -> do
    T.initTransactions first (C.Timeout 10000) >>= (@?= Nothing)
    T.beginTransaction first >>= (@?= Nothing)
    produceTo first topic 0 "fenced-output"
    withProducer testEnv "fenced" True (teBrokers testEnv) $ \replacement -> do
      T.initTransactions replacement (C.Timeout 10000) >>= (@?= Nothing)
      withTxFault 0 $ do
        T.commitTransaction first (C.Timeout 10000) >>= checkTx C.RdKafkaRespErrFenced (True, False, False)
        balanced 1
      T.beginTransaction replacement >>= (@?= Nothing)
      produceTo replacement topic 0 "replacement-output"
      T.commitTransaction replacement (C.Timeout 10000) >>= noTxError
      visible testEnv C.CallbackPollModeSync "fencing" topic "read_committed" ["replacement-output"]

abortRecovery :: TestEnv -> IO ()
abortRecovery testEnv = withProducer testEnv "small-admin" False (teBrokers testEnv) $ \admin -> do
  let topic = C.TopicName (prefixedName testEnv "tx-small-records")
      request = Topic.NewTopic topic (Topic.PartitionCount 1) (Topic.ReplicationFactor 1) (M.singleton "max.message.bytes" "1024")
  bracket_
    (Topic.createTopic admin request >>= either throwIO (const (pure ())))
    (void (Topic.deleteTopic admin topic))
    $ withProducer testEnv "abort-recovery" True (teBrokers testEnv)
    $ \producer -> do
      T.initTransactions producer (C.Timeout 10000) >>= (@?= Nothing)
      T.beginTransaction producer >>= (@?= Nothing)
      report <- newEmptyMVar
      let record = P.ProducerRecord topic (P.SpecifiedPartition 0) Nothing (Just (BS.replicate 4096 97)) mempty
      P.produceMessage' producer record (putMVar report) >>= (@?= Right ())
      P.flushProducerTimeout producer (C.Timeout 10000) >>= (@?= Nothing)
      bounded (readMVar report) >>= (@?= P.DeliveryFailure record (C.KafkaResponseError C.RdKafkaRespErrMsgSizeTooLarge))
      withTxFault 0 $ do
        T.commitTransaction producer (C.Timeout 10000) >>= checkTx C.RdKafkaRespErrMsgSizeTooLarge (False, False, True)
        balanced 1
      T.abortTransaction producer (C.Timeout 10000) >>= (@?= Nothing)
      T.beginTransaction producer >>= (@?= Nothing)
      let recovered = P.ProducerRecord topic (P.SpecifiedPartition 0) Nothing (Just "recovered-output") mempty
      recovery <- newEmptyMVar
      P.produceMessage' producer recovered (putMVar recovery) >>= (@?= Right ())
      P.flushProducerTimeout producer (C.Timeout 10000) >>= (@?= Nothing)
      bounded (readMVar recovery) >>= \case
        P.DeliverySuccess actual _ -> actual @?= recovered
        P.DeliveryFailure actual (C.KafkaResponseError C.RdKafkaRespErrPurgeQueue) -> do
          actual @?= recovered
          -- Apache 4.4/native 2.16 can reject this first recovery with a
          -- sequence error. An independent C probe reproduces the exact
          -- purge/abort-required sequence. Honor that native decision once;
          -- no arbitrary delivery error or unbounded retry is accepted.
          withTxFault 0 $ do
            T.commitTransaction producer (C.Timeout 10000) >>= checkTx C.RdKafkaRespErrOutOfOrderSequenceNumber (False, False, True)
            balanced 1
          T.abortTransaction producer (C.Timeout 10000) >>= (@?= Nothing)
          T.beginTransaction producer >>= (@?= Nothing)
          produceTo producer topic 0 "recovered-output"
          putStrLn "[transaction-recovery] observed purge and abort-required sequence error; next transaction delivered"
        other -> assertFailure (show other)
      T.commitTransaction producer (C.Timeout 10000) >>= noTxError
      visible testEnv C.CallbackPollModeSync "abort-recovery" topic "read_committed" ["recovered-output"]
