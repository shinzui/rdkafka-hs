module Kafka.Integration.BrokerFailureTest (tests) where

import Control.Concurrent (forkIOWithUnmask, killThread, threadDelay)
import Control.Concurrent.MVar
import Control.Exception (AsyncException (ThreadKilled), SomeException, bracket, finally, fromException, throwIO, try)
import Control.Monad (forM_, forever, void, when)
import Data.ByteString (ByteString)
import Data.IORef
import Data.Int (Int64)
import Data.List (sort, sortOn)
import Data.Map.Strict qualified as M
import Data.Text qualified as T
import Kafka.Compatibility.KafkaFixture
import Kafka.Consumer qualified as C
import Kafka.Metadata qualified as D
import Kafka.Producer qualified as P
import Kafka.TestEnv
import Kafka.Topic qualified as A
import Kafka.Topic.Types
import Kafka.Transaction qualified as X
import Kafka.TransactionTestSupport
import Kafka.Types
import System.Timeout (timeout)
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit

bounded :: String -> IO a -> IO a
bounded label action = timeout 30000000 action >>= maybe (assertFailure label) pure

tests :: IO KafkaFixture -> TestTree
tests getFixture =
  testGroup
    "broker-failure"
    $ [ testCase (label <> " observed leader election preserves exact records and commits") $ do
          fixture <- getFixture
          let environment = fixtureEnv fixture
              topic = TopicName (prefixedName environment ("leader-" <> T.pack label))
              properties = P.brokersList (teBrokers environment) <> P.extraProps (M.fromList [("enable.idempotence", "true"), ("acks", "all"), ("message.timeout.ms", "15000")])
          bracket (P.newProducer properties >>= either throwIO pure) releaseProducer $ \producer -> do
            A.createTopic producer (NewTopic topic (PartitionCount 1) (ReplicationFactor 3) (M.singleton "min.insync.replicas" "2")) >>= (@?= Right topic)
            flip finally (A.deleteTopic producer topic >>= (@?= Right topic)) $ do
              let props = C.brokersList (teBrokers environment) <> C.groupId (makeGroupId environment ("leader-" <> T.pack label)) <> C.noAutoCommit <> C.noAutoOffsetStore <> C.callbackPollMode mode
              bracket (C.newAssignedConsumer props >>= either throwIO pure) releaseConsumer $ \consumer -> do
                C.assign consumer [C.TopicPartition topic (PartitionId 0) C.PartitionOffsetBeginning] >>= (@?= Nothing)
                sendExact producer topic 0
                first <- receiveExact consumer topic 0
                C.commitOffsetMessage C.OffsetCommit consumer first >>= (@?= Nothing)
                verifyCommit consumer topic 1
                before <- awaitMetadata producer topic (\p -> D.pmError p == Nothing && sort (D.pmInSyncReplicas p) == map BrokerId [1, 2, 3])
                sort (D.pmReplicas before) @?= map BrokerId [1, 2, 3]
                let old = D.pmLeader before
                assertBool (show before) (old `elem` map BrokerId [1, 2, 3])
                putStrLn ("[leader-before] " <> label <> " " <> show before)
                flip finally (startBroker fixture old >> awaitMetadata producer topic (\p -> D.pmError p == Nothing && sort (D.pmInSyncReplicas p) == map BrokerId [1, 2, 3])) $ do
                  stopBroker fixture old
                  changed <- awaitMetadata producer topic (\p -> D.pmError p == Nothing && D.pmLeader p /= old && D.pmLeader p `elem` filter (/= old) (map BrokerId [1, 2, 3]) && sort (D.pmInSyncReplicas p) == filter (/= old) (map BrokerId [1, 2, 3]))
                  putStrLn ("[leader-elected] " <> label <> " " <> show changed)
                  sendExact producer topic 1
                  second <- receiveExact consumer topic 1
                  C.commitOffsetMessage C.OffsetCommit consumer second >>= (@?= Nothing)
                  verifyCommit consumer topic 2
                  C.pollMessage consumer (Timeout 100) >>= (@?= Left (KafkaResponseError C.RdKafkaRespErrTimedOut))
                  putStrLn ("[leader-recovered] " <> label <> " records=[0,1] committed=2")
      | (label, mode) <- [("async", C.CallbackPollModeAsync), ("sync", C.CallbackPollModeSync)]
      ]
      <> distributedTests getFixture
      <> transactionTests getFixture
      <> [ testCase "cancellation during complete outage restores all owned brokers" $ do
             fixture <- getFixture
             stopped <- newEmptyMVar
             completed <- newEmptyMVar
             worker <- forkIOWithUnmask $ \unmask -> do
               outcome <- try @SomeException $ unmask $ withBrokersStopped fixture $ do
                 assertBool "some owned broker remained running" =<< fixtureAllStopped fixture
                 putMVar stopped ()
                 threadDelay maxBound
               putMVar completed outcome
             flip finally (killThread worker) $ do
               bounded "owned outage did not begin" (takeMVar stopped)
               killThread worker
               bounded "owned outage cancellation did not finish restoration" (takeMVar completed) >>= \case
                 Left exception -> fromException exception @?= Just ThreadKilled
                 Right () -> assertFailure "outage worker returned without cancellation"
               awaitBrokers fixture
               assertBool "restored topology has no live brokers" . not =<< fixtureAllStopped fixture
               putStrLn "[outage-cancellation] ThreadKilled restored all three owned broker/controllers before healthy native metadata"
         ]

sendExact :: P.KafkaProducer -> TopicName -> Int64 -> IO ()
sendExact producer topic offset = do
  report <- newEmptyMVar
  let record = P.ProducerRecord topic (P.SpecifiedPartition 0) (Just "leader-key") (Just (if offset == 0 then "before" else "after")) (headersFromList [("origin", "mp3")])
  P.produceMessage' producer record (putMVar report) >>= (@?= Right ())
  P.flushProducerTimeout producer (Timeout 15000) >>= (@?= Nothing)
  bounded "missing leader-change delivery report" (takeMVar report) >>= (@?= P.DeliverySuccess record (C.Offset offset))

receiveExact :: C.KafkaConsumer -> TopicName -> Int64 -> IO (C.ConsumerRecord (Maybe ByteString) (Maybe ByteString))
receiveExact consumer topic offset = bounded "missing leader-change record" go
  where
    go =
      C.pollMessage consumer (Timeout 1000) >>= \case
        Left (KafkaResponseError C.RdKafkaRespErrTimedOut) -> go
        Left err -> throwIO err
        Right record -> do
          (C.crTopic record, C.crPartition record, C.crOffset record, C.crKey record, C.crValue record, C.crHeaders record)
            @?= (topic, PartitionId 0, C.Offset offset, Just "leader-key", Just (if offset == 0 then "before" else "after"), headersFromList [("origin", "mp3")])
          pure record

verifyCommit :: C.KafkaConsumer -> TopicName -> Int64 -> IO ()
verifyCommit consumer topic offset = C.committed consumer (Timeout 5000) [(topic, PartitionId 0)] >>= (@?= Right [C.TopicPartition topic (PartitionId 0) (C.PartitionOffset offset)])

awaitMetadata :: P.KafkaProducer -> TopicName -> (D.PartitionMetadata -> Bool) -> IO D.PartitionMetadata
awaitMetadata producer topic predicate = bounded "owned leader/ISR transition was not observed" go
  where
    go = do
      result <- D.topicMetadata producer (Timeout 2000) topic >>= either throwIO pure
      case D.kmTopics result of
        [metadata] | D.tmError metadata == Nothing -> case D.tmPartitions metadata of
          [partition] | predicate partition -> pure partition
          _ -> threadDelay 100000 >> go
        _ -> assertFailure (show result)

type Record = C.ConsumerRecord (Maybe ByteString) (Maybe ByteString)

distributedTests :: IO KafkaFixture -> [TestTree]
distributedTests getFixture =
  [ testCase (protocol <> " " <> label <> " handoff pause seek eviction and complete outage recover exact offsets") $ do
      fixture <- getFixture
      let environment = fixtureEnv fixture
          suffix = T.pack (protocol <> "-" <> label)
          group = makeGroupId environment ("distributed-" <> suffix)
      events <- newIORef ([] :: [(String, C.RebalanceEvent, Bool)])
      records <- newIORef []
      withReplicatedTopic fixture ("distributed-" <> suffix) 2 $ \topic producer -> do
        let callback role consumer event = do
              lost <- C.assignmentLost consumer
              atomicModifyIORef' events (\seen -> (seen <> [(role, event, lost)], ()))
            properties role = C.brokersList (teBrokers environment) <> C.groupId group <> C.noAutoCommit <> C.noAutoOffsetStore <> C.callbackPollMode mode <> C.extraProps (M.fromList [("group.protocol", "classic"), ("partition.assignment.strategy", T.pack strategy), ("session.timeout.ms", "6000"), ("heartbeat.interval.ms", "1000"), ("max.poll.interval.ms", "10000"), ("log_level", "0")]) <> C.setCallback (C.rebalanceCallback (callback role))
            acquire role = C.newConsumer (properties role) (C.topics [topic] <> C.offsetReset C.Earliest) >>= either throwIO pure
        bracket (acquire "first") releaseConsumer $ \first ->
          withPolling mode "first" first records $ \stopFirst -> do
            awaitAssignment first topic [PartitionId 0, PartitionId 1]
            bracket (acquire "second") releaseConsumer $ \second ->
              withPolling mode "second" second records $ \_ -> do
                (ownedFirst, ownedSecond) <-
                  bounded "group did not split its exact assignment" $
                    let await = do
                          a <- assigned first topic
                          b <- assigned second topic
                          if length a == 1 && length b == 1 && sort (a <> b) == [PartitionId 0, PartitionId 1] then pure (a, b) else threadDelay 100000 >> await
                     in await
                putStrLn ("[distributed-split] " <> protocol <> " " <> label <> " " <> show (ownedFirst, ownedSecond))
                healthy <- readIORef events
                assertBool (show healthy) (not (null healthy) && all (\(_, _, lost) -> not lost) healthy)
                forM_ [0, 1] $ \partition -> sendDistributed producer topic partition "before"
                takeRecords records 2 >>= assertRecords topic [(0, 0, "before"), (1, 0, "before")]
                (paused, open) <- case (ownedFirst, ownedSecond) of
                  ([a], [b]) -> pure (a, b)
                  actual -> assertFailure (show actual)
                putStrLn "[distributed-before] exact initial records committed"
                C.pausePartitions first [(topic, paused)] >>= (@?= KafkaResponseError C.RdKafkaRespErrNoError)
                sendDistributed producer topic (unPartitionId paused) "paused"
                sendDistributed producer topic (unPartitionId open) "open"
                takeRecords records 1 >>= assertRecords topic [(unPartitionId open, 1, "open")]
                threadDelay 200000
                readIORef records >>= (@?= [])
                C.seekPartitions first [C.TopicPartition topic paused C.PartitionOffsetBeginning] (Timeout 10000) >>= (@?= Nothing)
                C.resumePartitions first [(topic, paused)] >>= (@?= KafkaResponseError C.RdKafkaRespErrNoError)
                takeRecords records 2 >>= assertRecords topic [(unPartitionId paused, 0, "before"), (unPartitionId paused, 1, "paused")]
                putStrLn "[distributed-pause-seek] exact replay and unpaused partition delivered"
                stopFirst
                releaseConsumer first
                awaitAssignment second topic [PartitionId 0, PartitionId 1]
                forM_ [0, 1] $ \partition -> sendDistributed producer topic partition "handoff"
                takeRecords records 2 >>= assertRecords topic [(0, 2, "handoff"), (1, 2, "handoff")]
                verifyAllCommits second topic 2
                putStrLn "[distributed-handoff] second consumer owns both partitions; stored=3, committed=2"
                withBrokersStopped fixture $ do
                  assertBool "some owned broker remained running" =<< fixtureAllStopped fixture
                  putStrLn "[distributed-stopped] all owned processes exited"
                  result <- C.commitAllOffsets C.OffsetCommit second
                  result @?= Just (KafkaResponseError C.RdKafkaRespErrWaitCoord)
                  bounded "actual involuntary assignment loss was not observed" $
                    let await = do
                          seen <- readIORef events
                          if any (\(role, event, lost) -> role == "second" && lost && beforeRevoke event) seen then pure () else threadDelay 100000 >> await
                     in await
                  outageDelivery environment topic
                  C.assignment second >>= (@?= Right M.empty)
                  C.commitAllOffsets C.OffsetCommit second >>= (@?= Just (KafkaResponseError C.RdKafkaRespErrNoOffset))
                  putStrLn ("[distributed-outage] " <> protocol <> " " <> label <> " assignmentLost=True; commit-all=" <> show result)
                awaitBrokers fixture
                awaitAssignment second topic [PartitionId 0, PartitionId 1]
                -- The offline commit failed with every broker stopped. Replay
                -- exactly the uncommitted handoff records, then commit them.
                takeRecords records 2 >>= assertRecords topic [(0, 2, "handoff"), (1, 2, "handoff")]
                C.commitAllOffsets C.OffsetCommit second >>= (@?= Nothing)
                verifyAllCommits second topic 3
                forM_ [0, 1] $ \partition -> sendDistributed producer topic partition "recovered"
                takeRecords records 2 >>= assertRecords topic [(0, 3, "recovered"), (1, 3, "recovered")]
                C.commitAllOffsets C.OffsetCommit second >>= (@?= Nothing)
                verifyAllCommits second topic 4
                threadDelay 200000
                readIORef records >>= (@?= [])
                C.assignmentLost second >>= (@?= False)
                putStrLn ("[distributed-recovered] " <> protocol <> " " <> label <> " partitions=[0,1] committed=[4,4]; no expired or duplicate record")
  | (protocol, strategy) <- [("eager", "range"), ("cooperative", "cooperative-sticky")],
    (label, mode) <- [("async", C.CallbackPollModeAsync), ("sync", C.CallbackPollModeSync)]
  ]
  where
    beforeRevoke (C.RebalanceBeforeRevoke _) = True
    beforeRevoke _ = False

withReplicatedTopic :: KafkaFixture -> T.Text -> Int -> (TopicName -> P.KafkaProducer -> IO a) -> IO a
withReplicatedTopic fixture suffix partitions action =
  let environment = fixtureEnv fixture
      topic = TopicName (prefixedName environment suffix)
      props = P.brokersList (teBrokers environment) <> P.extraProps (M.fromList [("enable.idempotence", "true"), ("acks", "all"), ("message.timeout.ms", "15000"), ("log_level", "0")])
   in bracket (P.newProducer props >>= either throwIO pure) releaseProducer $ \producer -> do
        A.createTopic producer (NewTopic topic (PartitionCount partitions) (ReplicationFactor 3) (M.singleton "min.insync.replicas" "2")) >>= (@?= Right topic)
        action topic producer `finally` (A.deleteTopic producer topic >>= (@?= Right topic))

assigned :: C.KafkaConsumer -> TopicName -> IO [PartitionId]
assigned consumer topic = sort . M.findWithDefault [] topic <$> (C.assignment consumer >>= either throwIO pure)

awaitAssignment :: C.KafkaConsumer -> TopicName -> [PartitionId] -> IO ()
awaitAssignment consumer topic expected = bounded "exact group assignment was not observed" go
  where
    go = assigned consumer topic >>= \actual -> if actual == expected then pure () else threadDelay 100000 >> go

withPolling :: C.CallbackPollMode -> String -> C.KafkaConsumer -> IORef [(String, Record)] -> (IO () -> IO a) -> IO a
withPolling mode role consumer records action = do
  done <- newEmptyMVar
  let loop = forever $ do
        when (mode == C.CallbackPollModeSync) (C.pollConsumerEvents consumer (Just (Timeout 0)))
        C.pollMessage consumer (Timeout 100) >>= \case
          Left (KafkaResponseError code) | code `elem` [C.RdKafkaRespErrTimedOut, C.RdKafkaRespErrPartitionEof, C.RdKafkaRespErrTransport, C.RdKafkaRespErrAllBrokersDown] -> pure ()
          Left err -> throwIO err
          Right record -> do
            C.storeOffsetMessage consumer record >>= (@?= Nothing)
            -- These phases deliberately leave stored offsets for commit-all
            -- and demonstrate exact replay after its failed offline request.
            when (C.crValue record `notElem` [Just "handoff", Just "recovered"]) $
              C.commitOffsetMessage C.OffsetCommit consumer record >>= (@?= Nothing)
            atomicModifyIORef' records (\seen -> (seen <> [(role, record)], ()))
  bracket
    (forkIOWithUnmask (\unmask -> try @SomeException (unmask loop) >>= putMVar done))
    ( \worker -> do
        killThread worker
        bounded "consumer polling worker did not stop" (readMVar done) >>= \case
          Left exception | (fromException exception :: Maybe AsyncException) == Just ThreadKilled -> pure ()
          Left exception -> throwIO exception
          Right _ -> assertFailure "polling worker stopped unexpectedly"
    )
    (\worker -> action (killThread worker >> void (bounded "first consumer worker did not stop" (readMVar done))))

takeRecords :: IORef [(String, Record)] -> Int -> IO [(String, Record)]
takeRecords reference count = bounded "exact distributed records did not arrive" go
  where
    go = do
      seen <- readIORef reference
      if length seen >= count then atomicModifyIORef' reference (\values -> ([], values)) else threadDelay 10000 >> go

assertRecords :: TopicName -> [(Int, Int64, ByteString)] -> [(String, Record)] -> IO ()
assertRecords topic expected actual =
  order [(C.crTopic record, C.crPartition record, C.crOffset record, C.crKey record, C.crValue record, C.crHeaders record) | (_, record) <- actual]
    @?= order [(topic, PartitionId partition, C.Offset offset, Just "distributed-key", Just value, headersFromList [("origin", "mp3-distributed")]) | (partition, offset, value) <- expected]
  where
    order = sortOn (\(_, partition, offset, _, _, _) -> (unPartitionId partition, C.unOffset offset))

sendDistributed :: P.KafkaProducer -> TopicName -> Int -> ByteString -> IO ()
sendDistributed producer topic partition value = do
  report <- newEmptyMVar
  let record = P.ProducerRecord topic (P.SpecifiedPartition partition) (Just "distributed-key") (Just value) (headersFromList [("origin", "mp3-distributed")])
  P.produceMessage' producer record (putMVar report) >>= (@?= Right ())
  P.flushProducerTimeout producer (Timeout 15000) >>= (@?= Nothing)
  bounded "missing distributed delivery" (takeMVar report) >>= \case
    P.DeliverySuccess actual _ -> actual @?= record
    result -> assertFailure (show result)

verifyAllCommits :: C.KafkaConsumer -> TopicName -> Int64 -> IO ()
verifyAllCommits consumer topic offset = C.committed consumer (Timeout 5000) [(topic, PartitionId 0), (topic, PartitionId 1)] >>= (@?= Right [C.TopicPartition topic (PartitionId partition) (C.PartitionOffset offset) | partition <- [0, 1]])

outageDelivery :: TestEnv -> TopicName -> IO ()
outageDelivery environment topic = do
  let properties = P.brokersList (teBrokers environment) <> P.extraProps (M.fromList [("queue.buffering.max.messages", "1"), ("message.timeout.ms", "2000"), ("log_level", "0")])
      record = P.ProducerRecord topic (P.SpecifiedPartition 0) Nothing (Just "must-expire-during-outage") mempty
  bracket (P.newProducer properties >>= either throwIO pure) releaseProducer $ \producer -> do
    report <- newEmptyMVar
    unexpected <- newEmptyMVar
    P.produceMessage' producer record (putMVar report) >>= (@?= Right ())
    P.produceMessage' producer record (putMVar unexpected) >>= (@?= Left (P.ImmediateError (KafkaResponseError C.RdKafkaRespErrQueueFull)))
    P.flushProducerTimeout producer (Timeout 5000) >>= (@?= Nothing)
    bounded "outage delivery timeout report missing" (takeMVar report) >>= (@?= P.DeliveryFailure record (KafkaResponseError C.RdKafkaRespErrMsgTimedOut))
    tryReadMVar unexpected >>= (@?= Nothing)

transactionTests :: IO KafkaFixture -> [TestTree]
transactionTests getFixture =
  [ testCase (label <> " transaction commit timeout across full outage retries and preserves read-committed visibility") $ do
      fixture <- getFixture
      let environment = fixtureEnv fixture
      withReplicatedTopic fixture ("transaction-outage-" <> T.pack label) 1 $ \topic _ -> do
        let props = P.brokersList (teBrokers environment) <> P.extraProps (M.fromList [("transactional.id", prefixedName environment ("transaction-outage-" <> T.pack label)), ("transaction.timeout.ms", "60000"), ("message.timeout.ms", "30000"), ("log_level", "0")])
        bracket (P.newProducer props >>= either throwIO pure) releaseProducer $ \producer -> do
          X.initTransactions producer (Timeout 15000) >>= (@?= Nothing)
          X.beginTransaction producer >>= (@?= Nothing)
          sendDistributed producer topic 0 "committed-after-outage"
          let visibilityProperties = C.brokersList (teBrokers environment) <> C.groupId (makeGroupId environment ("transaction-visibility-" <> T.pack label)) <> C.callbackPollMode mode <> C.noAutoCommit <> C.extraProp "isolation.level" "read_committed" <> C.extraProp "log_level" "0"
          bracket (C.newAssignedConsumer visibilityProperties >>= either throwIO pure) releaseConsumer $ \consumer -> do
            C.assign consumer [C.TopicPartition topic (PartitionId 0) C.PartitionOffsetBeginning] >>= (@?= Nothing)
            C.pollMessage consumer (Timeout 500) >>= (@?= Left (KafkaResponseError C.RdKafkaRespErrTimedOut))
            withBrokersStopped fixture $ withTxFault 0 $ do
              X.commitTransaction producer (Timeout 1000) >>= assertRetry
              txStat 0 >>= (@?= 1)
              txStat 1 >>= (@?= 1)
              txStat 8 >>= (@?= 0)
            awaitBrokers fixture
            bounded "transaction did not finish after owned broker restoration" $
              let retry =
                    X.commitTransaction producer (Timeout 10000) >>= \case
                      Nothing -> pure ()
                      result -> assertRetry result >> retry
               in retry
            record <-
              bounded "committed transaction record was not visible" $
                let receive =
                      C.pollMessage consumer (Timeout 1000) >>= \case
                        Left (KafkaResponseError C.RdKafkaRespErrTimedOut) -> receive
                        Left err -> throwIO err
                        Right value -> pure value
                 in receive
            assertRecords topic [(0, 0, "committed-after-outage")] [(label, record)]
            C.pollMessage consumer (Timeout 200) >>= (@?= Left (KafkaResponseError C.RdKafkaRespErrTimedOut))
            putStrLn ("[transaction-outage] " <> label <> " timed-out commit retriable=True abort=False fatal=False; restored commit publishes exact offset 0 once")
  | (label, mode) <- [("async", C.CallbackPollModeAsync), ("sync", C.CallbackPollModeSync)]
  ]
  where
    assertRetry Nothing = assertFailure "offline transaction unexpectedly committed"
    assertRetry (Just err) = (X.getKafkaError err, X.kafkaErrorIsFatal err, X.kafkaErrorIsRetriable err, X.kafkaErrorTxnRequiresAbort err) @?= (KafkaResponseError C.RdKafkaRespErrTimedOut, False, True, False)
