module Kafka.Integration.AuthenticationTest (tests) where

import Control.Concurrent (forkFinally, killThread, threadDelay)
import Control.Concurrent.MVar
import Control.Exception (AsyncException (ThreadKilled), bracket, finally, fromException, throwIO)
import Control.Monad (forM_, void, when)
import Data.ByteString (ByteString)
import Data.Map.Strict qualified as M
import Data.Text qualified as T
import Kafka.Compatibility.KafkaFixture
import Kafka.Compatibility.SecurityFixture
import Kafka.Consumer qualified as C
import Kafka.Producer qualified as P
import Kafka.TestEnv
import Kafka.Topic qualified as A
import Kafka.Topic.Types
import Kafka.Types
import System.Directory (doesDirectoryExist, doesFileExist)
import System.Timeout (timeout)
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit

tests :: IO SecurityFixture -> TestTree
tests getFixture =
  testGroup
    "authentication-contract"
    $ [ testCase "verified TLS authenticated delivery and exact consume/commit in both modes" $ do
          fixture <- getFixture
          let environment = fixtureEnv (securityKafka fixture)
              topic = securityTopic fixture "primary"
              record = secureRecord topic
          bracket (newProducer fixture "writer") releaseProducer $ \producer -> do
            report <- newEmptyMVar
            P.produceMessage' producer record (putMVar report) >>= (@?= Right ())
            P.flushProducerTimeout producer (Timeout 10000) >>= (@?= Nothing)
            bounded "authenticated delivery report" (takeMVar report) >>= (@?= P.DeliverySuccess record (C.Offset 0))
          forM_ [("async", C.CallbackPollModeAsync), ("sync", C.CallbackPollModeSync)] $ \(label, mode) -> do
            let properties = C.brokersList (teBrokers environment) <> C.groupId (makeGroupId environment ("security-" <> label)) <> C.noAutoCommit <> C.noAutoOffsetStore <> C.callbackPollMode mode <> C.extraProps (securitySettings fixture "reader")
            bracket (C.newAssignedConsumer properties >>= either throwIO pure) releaseConsumer $ \consumer -> do
              C.assign consumer [C.TopicPartition topic (PartitionId 0) C.PartitionOffsetBeginning] >>= (@?= Nothing)
              received <- bounded "authenticated record" (C.pollMessage consumer (Timeout 10000)) >>= either throwIO pure
              (C.crTopic received, C.crPartition received, C.crOffset received, C.crKey received, C.crValue received, C.crHeaders received)
                @?= (topic, PartitionId 0, C.Offset 0, Just "auth-key", Just "authenticated-record", headersFromList [("security", "verified-tls")])
              C.commitOffsetMessage C.OffsetCommit consumer received >>= (@?= Nothing)
              C.committed consumer (Timeout 5000) [(topic, PartitionId 0)] >>= (@?= Right [C.TopicPartition topic (PartitionId 0) (C.PartitionOffset 1)]),
        testCase "authenticated identity without topic ACL receives exact produce denial" $ do
          fixture <- getFixture
          let record = secureRecord (securityTopic fixture "primary")
          bracket (newProducer fixture "denied") releaseProducer $ \producer -> do
            report <- newEmptyMVar
            P.produceMessage' producer record (putMVar report) >>= (@?= Right ())
            P.flushProducerTimeout producer (Timeout 10000) >>= (@?= Nothing)
            bounded "denied delivery report" (takeMVar report) >>= (@?= P.DeliveryFailure record (KafkaResponseError C.RdKafkaRespErrTopicAuthorizationFailed))
      ]
      <> rejectionTests getFixture
      <> reconnectTests getFixture
      <> [ testCase "interruption destroys owned secure processes, credentials, ACL state and private keys" $ do
             ready <- newEmptyMVar
             blocked <- newEmptyMVar
             done <- newEmptyMVar
             worker <-
               forkFinally
                 ( bracket createSecurityFixture destroySecurityFixture $ \fixture ->
                     bracket (newProducer fixture "writer") releaseProducer $ \producer -> do
                       void (publish producer (secureRecord (securityTopic fixture "primary")))
                       putMVar ready fixture
                       takeMVar blocked
                 )
                 (putMVar done)
             let await io = timeout 45000000 io >>= maybe (assertFailure "secure interruption deadline") pure
             flip finally (killThread worker) $ do
               fixture <- await (takeMVar ready)
               await (killThread worker)
               await (readMVar done) >>= \case
                 Left exception -> (fromException exception :: Maybe AsyncException) @?= Just ThreadKilled
                 Right _ -> assertFailure "secure fixture finished without interruption"
               let kafka = securityKafka fixture
                   root = fixtureRoot kafka
               fixtureAllStopped kafka >>= (@?= True)
               doesDirectoryExist (root <> "/secret") >>= (@?= False)
               doesFileExist (root <> "/node-1.properties") >>= (@?= False)
               doesDirectoryExist (root <> "/node-1-data") >>= (@?= False)
               doesFileExist (root <> "/security-cleanup.txt") >>= (@?= True)
               putStrLn "[security-interruption] all owned processes exited; credentials, private keys, JAAS and broker ACL state removed"
         ]

reconnectTests :: IO SecurityFixture -> [TestTree]
reconnectTests getFixture =
  [ testCase "producer credential replacement authenticates a new identity after observed reconnect and recovers from invalid replacement" $ do
      fixture <- getFixture
      observed <- newEmptyMVar
      let kafka = securityKafka fixture
          environment = fixtureEnv kafka
          authenticationError = KafkaResponseError C.RdKafkaRespErrAuthentication
          settings = M.insert "message.timeout.ms" "10000" (M.insert "topic.metadata.refresh.interval.ms" "1000" (securitySettings fixture "writer"))
          properties = P.brokersList (teBrokers environment) <> P.extraProps settings <> P.setCallback (P.errorCallback (\err _ -> when (err == authenticationError) (void (tryPutMVar observed err))))
      bracket (P.newProducer properties >>= either throwIO pure) releaseProducer $ \producer -> do
        void (publish producer (secureRecord (securityTopic fixture "primary")))
        withSecurityCredentials fixture "writer2" $ \username _ -> P.setSaslCredentials producer username "deliberately-invalid-replacement" >>= (@?= Nothing)
        restartOwned kafka
        reports <- newEmptyMVar
        let record = secureRecord (securityTopic fixture "replacement")
        P.produceMessage' producer record (putMVar reports) >>= (@?= Right ())
        let awaitAuthentication =
              tryReadMVar observed >>= \case
                Just err -> err @?= authenticationError
                Nothing -> void (P.flushProducerTimeout producer (Timeout 100)) >> threadDelay 10000 >> awaitAuthentication
        bounded "producer did not authenticate with the invalid replacement" awaitAuthentication
        withSecurityCredentials fixture "writer2" (P.setSaslCredentials producer) >>= (@?= Nothing)
        P.flushProducerTimeout producer (Timeout 15000) >>= (@?= Nothing)
        bounded "replacement identity did not deliver" (takeMVar reports) >>= \case
          P.DeliverySuccess actual _ -> actual @?= record
          _ -> assertFailure "corrected producer credentials failed the replacement-only ACL"
        putStrLn "[security-reconnect] producer observed invalid authentication then replacement-only authorized delivery"
  ]
    <> [ testCase (label <> " consumer credential replacement authenticates after observed reconnect and recovers from invalid replacement") $ do
           fixture <- getFixture
           let kafka = securityKafka fixture
               environment = fixtureEnv kafka
               primary = securityTopic fixture "primary"
               replacement = securityTopic fixture "replacement"
               authenticationError = KafkaResponseError C.RdKafkaRespErrAuthentication
           firstOffset <- bracket (newProducer fixture "writer") releaseProducer $ \producer -> publish producer (secureRecord primary)
           nextOffset <- bracket (newProducer fixture "writer2") releaseProducer $ \producer -> publish producer (secureRecord replacement)
           observed <- newEmptyMVar
           let properties = C.brokersList (teBrokers environment) <> C.groupId (makeGroupId environment ("security-reconnect-" <> T.pack label)) <> C.noAutoCommit <> C.noAutoOffsetStore <> C.callbackPollMode mode <> C.extraProps (M.insert "topic.metadata.refresh.interval.ms" "1000" (securitySettings fixture "reader")) <> C.setCallback (C.errorCallback (\err _ -> when (err == authenticationError) (void (tryPutMVar observed err))))
           bracket (C.newAssignedConsumer properties >>= either throwIO pure) releaseConsumer $ \consumer -> do
             assignAt consumer primary firstOffset
             void (receiveAt consumer primary firstOffset)
             withSecurityCredentials fixture "reader2" $ \username _ -> C.setSaslCredentials consumer username "deliberately-invalid-replacement" >>= (@?= Nothing)
             restartOwned kafka
             assignAt consumer replacement nextOffset
             let awaitAuthentication =
                   tryReadMVar observed >>= \case
                     Just err -> err @?= authenticationError
                     Nothing -> do
                       C.pollMessage consumer (Timeout 100) >>= \case
                         Left err | err == authenticationError -> void (tryPutMVar observed err)
                         Right _ -> assertFailure "invalid replacement fetched a record"
                         _ -> pure ()
                       awaitAuthentication
             bounded "consumer did not authenticate with invalid replacement" awaitAuthentication
             withSecurityCredentials fixture "reader2" (C.setSaslCredentials consumer) >>= (@?= Nothing)
             assignAt consumer replacement nextOffset
             record <- receiveAt consumer replacement nextOffset
             C.commitOffsetMessage C.OffsetCommit consumer record >>= (@?= Nothing)
             C.committed consumer (Timeout 5000) [(replacement, PartitionId 0)] >>= (@?= Right [C.TopicPartition replacement (PartitionId 0) (C.PartitionOffset (C.unOffset nextOffset + 1))])
             putStrLn ("[security-reconnect] consumer " <> label <> " observed invalid authentication then replacement-only record and exact next commit")
       | (label, mode) <- [("async", C.CallbackPollModeAsync), ("sync", C.CallbackPollModeSync)]
       ]

restartOwned :: KafkaFixture -> IO ()
restartOwned fixture = stopBroker fixture (BrokerId 1) `finally` (startBroker fixture (BrokerId 1) >> awaitBrokers fixture)

assignAt :: C.KafkaConsumer -> TopicName -> C.Offset -> IO ()
assignAt consumer topic offset = C.assign consumer [C.TopicPartition topic (PartitionId 0) (C.PartitionOffset (C.unOffset offset))] >>= (@?= Nothing)

receiveAt :: C.KafkaConsumer -> TopicName -> C.Offset -> IO (C.ConsumerRecord (Maybe ByteString) (Maybe ByteString))
receiveAt consumer topic offset = bounded "missing replacement-authorized record" go
  where
    go =
      C.pollMessage consumer (Timeout 1000) >>= \case
        Left (KafkaResponseError C.RdKafkaRespErrTimedOut) -> go
        Left (KafkaResponseError C.RdKafkaRespErrTopicAuthorizationFailed) -> go
        Left (KafkaResponseError C.RdKafkaRespErrAuthentication) -> go
        Left err -> throwIO err
        Right record -> do
          (C.crTopic record, C.crPartition record, C.crOffset record, C.crKey record, C.crValue record, C.crHeaders record) @?= (topic, PartitionId 0, offset, Just "auth-key", Just "authenticated-record", headersFromList [("security", "verified-tls")])
          pure record

rejectionTests :: IO SecurityFixture -> [TestTree]
rejectionTests getFixture =
  [ testCase (label <> " producer reports the exact connection failure and no delivery") $ do
      fixture <- getFixture
      let environment = fixtureEnv (securityKafka fixture)
          expected = KafkaResponseError code
          record = secureRecord (securityTopic fixture "primary")
      observed <- newEmptyMVar
      reports <- newEmptyMVar
      let properties = P.brokersList (addresses environment) <> P.extraProps (M.insert "message.timeout.ms" "5000" (change (securitySettings fixture "writer"))) <> P.setCallback (P.errorCallback (\err _ -> when (err == expected) (void (tryPutMVar observed err))))
      bracket (P.newProducer properties >>= either throwIO pure) releaseProducer $ \producer -> do
        P.produceMessage' producer record (putMVar reports) >>= (@?= Right ())
        P.flushProducerTimeout producer (Timeout 10000) >>= (@?= Nothing)
        bounded "missing expected producer connection error" (takeMVar observed) >>= (@?= expected)
        bounded "missing rejected delivery" (takeMVar reports) >>= (@?= P.DeliveryFailure record (KafkaResponseError C.RdKafkaRespErrMsgTimedOut))
  | (label, change, addresses, code) <- rejectionVariants
  ]
    <> [ testCase (label <> " " <> modeLabel <> " consumer reports the exact connection failure") $ do
           fixture <- getFixture
           let environment = fixtureEnv (securityKafka fixture)
               expected = KafkaResponseError code
               topic = securityTopic fixture "primary"
           observed <- newEmptyMVar
           let properties = C.brokersList (addresses environment) <> C.groupId (makeGroupId environment ("security-rejected-" <> T.pack label <> T.pack modeLabel)) <> C.noAutoCommit <> C.callbackPollMode mode <> C.extraProps (change (securitySettings fixture "reader")) <> C.setCallback (C.errorCallback (\err _ -> when (err == expected) (void (tryPutMVar observed err))))
           bracket (C.newAssignedConsumer properties >>= either throwIO pure) releaseConsumer $ \consumer -> do
             C.assign consumer [C.TopicPartition topic (PartitionId 0) C.PartitionOffsetBeginning] >>= (@?= Nothing)
             let await =
                   tryReadMVar observed >>= \case
                     Just actual -> actual @?= expected
                     Nothing -> do
                       C.pollMessage consumer (Timeout 100) >>= \case
                         Left err | err == expected -> void (tryPutMVar observed err)
                         Right _ -> assertFailure "rejected authentication/TLS unexpectedly fetched a record"
                         _ -> pure ()
                       threadDelay 10000
                       await
             bounded "missing expected consumer connection error" await
       | (label, change, addresses, code) <- rejectionVariants,
         (modeLabel, mode) <- [("async", C.CallbackPollModeAsync), ("sync", C.CallbackPollModeSync)]
       ]
    <> [ testCase (label <> " exact topic/group/admin authorization denials") $ do
           fixture <- getFixture
           let environment = fixtureEnv (securityKafka fixture)
               topic = securityTopic fixture "primary"
               props identity group = C.brokersList (teBrokers environment) <> C.groupId (makeGroupId environment group) <> C.noAutoCommit <> C.noAutoOffsetStore <> C.callbackPollMode mode <> C.extraProps (M.insert "session.timeout.ms" "6000" (securitySettings fixture identity))
           bracket (C.newAssignedConsumer (props "denied" "security-topic-denied") >>= either throwIO pure) releaseConsumer $ \consumer -> do
             C.assign consumer [C.TopicPartition topic (PartitionId 0) C.PartitionOffsetBeginning] >>= (@?= Nothing)
             let awaitDenial =
                   C.pollMessage consumer (Timeout 1000) >>= \case
                     Left (KafkaResponseError C.RdKafkaRespErrTimedOut) -> awaitDenial
                     result -> result @?= Left (KafkaResponseError C.RdKafkaRespErrTopicAuthorizationFailed)
             bounded "missing topic ACL denial" awaitDenial
           offset <- bracket (newProducer fixture "writer") releaseProducer $ \producer -> publish producer (secureRecord topic)
           bracket (C.newAssignedConsumer (props "reader" "forbidden-security-group") >>= either throwIO pure) releaseConsumer $ \consumer -> do
             C.assign consumer [C.TopicPartition topic (PartitionId 0) (C.PartitionOffset (C.unOffset offset))] >>= (@?= Nothing)
             record <- C.pollMessage consumer (Timeout 10000) >>= either throwIO pure
             -- A denied FindCoordinator leaves no coordinator for the native
             -- synchronous commit. Assert that exact result and the broker's
             -- exact authorization audit, rather than guessing GROUP_AUTH.
             C.commitOffsetMessage C.OffsetCommit consumer record >>= (@?= Just (KafkaResponseError C.RdKafkaRespErrWaitCoord))
             assertGroupAuthorizationDenied fixture "reader" (prefixedName environment "forbidden-security-group")
           bracket (newProducer fixture "denied") releaseProducer $ \producer -> do
             let deniedTopic = TopicName (prefixedName environment ("security-admin-denied-" <> T.pack label))
             A.createTopic producer (NewTopic deniedTopic (PartitionCount 1) (ReplicationFactor 1) M.empty) >>= (@?= Left (KafkaResponseError C.RdKafkaRespErrTopicAuthorizationFailed))
           bracket (newProducer fixture "admin") releaseProducer $ \producer -> do
             let allowedTopic = TopicName (prefixedName environment ("security-admin-allowed-" <> T.pack label))
             A.createTopic producer (NewTopic allowedTopic (PartitionCount 1) (ReplicationFactor 1) M.empty) >>= (@?= Right allowedTopic)
             A.deleteTopic producer allowedTopic >>= (@?= Right allowedTopic)
       | (label, mode) <- [("async", C.CallbackPollModeAsync), ("sync", C.CallbackPollModeSync)]
       ]

rejectionVariants :: [(String, M.Map T.Text T.Text -> M.Map T.Text T.Text, TestEnv -> [BrokerAddress], C.RdKafkaRespErrT)]
rejectionVariants =
  [ ("invalid-password", M.insert "sasl.password" "deliberately-invalid-fixture-password", teBrokers, C.RdKafkaRespErrAuthentication),
    ("untrusted-CA", M.delete "ssl.ca.location", teBrokers, C.RdKafkaRespErrSsl),
    ("hostname-mismatch", id, map (BrokerAddress . T.replace "localhost:" "127.0.0.1:" . unBrokerAddress) . teBrokers, C.RdKafkaRespErrSsl)
  ]

secureRecord :: TopicName -> P.ProducerRecord
secureRecord topic = P.ProducerRecord topic (P.SpecifiedPartition 0) (Just "auth-key") (Just "authenticated-record") (headersFromList [("security", "verified-tls")])

newProducer :: SecurityFixture -> String -> IO P.KafkaProducer
newProducer fixture label = P.newProducer (P.brokersList (teBrokers (fixtureEnv (securityKafka fixture))) <> P.extraProps (M.insert "message.timeout.ms" "5000" (securitySettings fixture label))) >>= either throwIO pure

bounded :: String -> IO a -> IO a
bounded label action = timeout 15000000 action >>= maybe (assertFailure label) pure

publish :: P.KafkaProducer -> P.ProducerRecord -> IO C.Offset
publish producer record = do
  report <- newEmptyMVar
  P.produceMessage' producer record (putMVar report) >>= (@?= Right ())
  P.flushProducerTimeout producer (Timeout 10000) >>= (@?= Nothing)
  bounded "missing authenticated delivery" (takeMVar report) >>= \case
    P.DeliverySuccess actual offset -> (actual @?= record) >> pure offset
    _ -> assertFailure "authenticated record was not delivered"
