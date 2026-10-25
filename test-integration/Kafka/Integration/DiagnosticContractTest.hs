module Kafka.Integration.DiagnosticContractTest (tests) where

import Control.Concurrent (forkFinally, forkIOWithUnmask, killThread, threadDelay)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, readMVar)
import Control.Exception (AsyncException (ThreadKilled), IOException, SomeException, bracket, finally, fromException, throwIO, try)
import Control.Monad (replicateM_)
import Data.ByteString.Char8 qualified as BS
import Data.Map.Strict qualified as M
import GHC.Conc (BlockReason (BlockedOnException), ThreadStatus (ThreadBlocked), threadStatus)
import Kafka.Consumer qualified as C
import Kafka.DiagnosticTestSupport
import Kafka.Dump
import Kafka.Producer qualified as P
import Kafka.ProducerTestSupport (nativeProducerProperty, withProducerTracking)
import Kafka.TestEnv
import System.Directory (getTemporaryDirectory, removeFile)
import System.IO (Handle, SeekMode (AbsoluteSeek), hClose, hFlush, hIsClosed, hPutStr, hSeek, hTell, openBinaryTempFile)
import System.IO.Error (ioeGetLocation)
import System.Timeout (timeout)
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertBool, testCase, (@?=))

tests :: TestEnv -> TestTree
tests env =
  testGroup "diagnostics" $
    [ testCase "supported-property print flushes and preserves the caller handle" $
        withDiagnosticFault 0 $
          withOutput $ \_ handle -> do
            hPutStr handle "before\n"
            hPrintSupportedKafkaConf handle
            hIsClosed handle >>= (@?= False)
            mapM diagnosticStat [2, 3] >>= (@?= [1, 1])
            output <- readOutput handle
            assertBool (show output) ("before\n" `BS.isPrefixOf` output && "bootstrap.servers" `BS.isInfixOf` output && "auto.offset.reset" `BS.isInfixOf` output)
            hPutStr handle "after\n"
            hFlush handle
            readOutput handle >>= \result -> assertBool (show result) ("after\n" `BS.isSuffixOf` result),
      testCase "client print flushes and preserves the caller handle" $
        bracket (P.newProducer (P.brokersList (teBrokers env) <> P.extraProp "client.id" "diagnostic-client") >>= either throwIO pure) releaseProducer $ \producer ->
          withDiagnosticFault 0 $ withOutput $ \_ handle -> do
            hPrintKafka handle producer
            hIsClosed handle >>= (@?= False)
            mapM diagnosticStat [2, 3] >>= (@?= [1, 1])
            output <- readOutput handle
            assertBool (show output) ("diagnostic-client" `BS.isInfixOf` output)
            hPutStr handle "after\n"
            hFlush handle
            readOutput handle >>= \result -> assertBool (show result) ("after\n" `BS.isSuffixOf` result),
      testCase "failed fdopen releases the duplicate and preserves the original" $
        withDiagnosticFault 3 $
          withOutput $ \_ handle -> do
            result <- try @IOException (hPrintSupportedKafkaConf handle)
            case result of
              Left exception -> ioeGetLocation exception @?= "fdopen"
              Right _ -> throwIO (userError "injected fdopen failure was suppressed")
            hIsClosed handle >>= (@?= False)
            mapM diagnosticStat [2, 3, 4] >>= (@?= [0, 0, 0])
            hPutStr handle "still usable"
            hFlush handle,
      testCase "failed native flush is reported after closing the duplicate" $
        withDiagnosticFault 4 $
          withOutput $ \_ handle -> do
            result <- try @IOException (hPrintSupportedKafkaConf handle)
            case result of
              Left exception -> ioeGetLocation exception @?= "fclose"
              Right _ -> throwIO (userError "injected fclose failure was suppressed")
            hIsClosed handle >>= (@?= False)
            mapM diagnosticStat [2, 3, 4] >>= (@?= [1, 1, 0])
            hPutStr handle "still usable"
            hFlush handle,
      testCase "producer configuration dumps retain exact explicit and global settings" $
        bracket (P.newProducer (P.brokersList (teBrokers env) <> P.extraProp "client.id" "diagnostic-global" <> P.extraTopicProps (M.fromList [("message.timeout.ms", "2345"), ("compression.codec", "gzip")])) >>= either throwIO pure) releaseProducer $ \producer ->
          withDiagnosticFault 0 $ do
            replicateM_ 100 $ do
              global <- dumpKafkaConf producer
              M.lookup "client.id" global @?= Just "diagnostic-global"
              topic <- dumpTopicConf producer
              M.lookup "message.timeout.ms" topic @?= Just "2345"
              M.lookup "compression.codec" topic @?= Just "gzip"
            mapM diagnosticStat [0, 1] >>= (@?= [200, 200]),
      testCase "producer constructor log level overrides the global property on the native client" $
        withTopic env "producer-constructor-log" 1 $ \topic _ ->
          bracket (P.newProducer (P.brokersList (teBrokers env) <> P.extraProp "log_level" "2" <> P.logLevel P.KafkaLogWarning) >>= either throwIO pure) releaseProducer $ \producer -> withProducerTracking 0 $ do
            produceTo producer topic 0 "native-log-level"
            nativeProducerProperty 5 >>= (@?= Just "4")
            config <- dumpKafkaConf producer
            M.lookup "log_level" config @?= Just "2",
      testCase "consumer configuration dump preserves global settings after subscription merge" $
        bracket (C.newAssignedConsumer (C.brokersList (teBrokers env) <> C.groupId (makeGroupId env "diagnostic") <> C.extraProp "client.id" "diagnostic-consumer" <> C.callbackPollMode C.CallbackPollModeSync) >>= either throwIO pure) releaseConsumer $ \consumer -> do
          config <- dumpKafkaConf consumer
          M.lookup "client.id" config @?= Just "diagnostic-consumer"
          M.lookup "group.id" config @?= Just (C.unConsumerGroupId (makeGroupId env "diagnostic")),
      testCase "native dump counter detects an unreleased array" $ withDiagnosticFault 0 $ do
        diagnosticLeakControl
        mapM diagnosticStat [0, 1] >>= (@?= [1, 0])
    ]
      <> [ testCase (label <> " cancellation at native acquisition releases its array") $
             bracket (P.newProducer (P.brokersList (teBrokers env)) >>= either throwIO pure) releaseProducer $ \producer ->
               withDiagnosticFault fault (cancelled (action producer))
         | (label, fault, action) <- [("global", 1, dumpKafkaConf), ("topic", 2, dumpTopicConf)]
         ]

cancelled :: IO a -> IO ()
cancelled action = do
  done <- newEmptyMVar
  worker <- forkIOWithUnmask $ \unmask -> try @SomeException (unmask action) >>= putMVar done
  let bounded operation = timeout 5000000 operation >>= maybe (throwIO (userError "dump worker timed out")) pure
  flip finally (releaseDiagnosticBoundary >> bounded (readMVar done)) $ do
    awaitDiagnosticBoundary
    mapM diagnosticStat [0, 1] >>= (@?= [1, 0])
    killed <- newEmptyMVar
    killer <- forkFinally (killThread worker) (putMVar killed)
    let pending = threadStatus killer >>= \status -> if status == ThreadBlocked BlockedOnException then pure () else threadDelay 1000 >> pending
    bounded pending
    releaseDiagnosticBoundary
    bounded (readMVar done) >>= \case
      Left exception -> (fromException exception :: Maybe AsyncException) @?= Just ThreadKilled
      Right _ -> throwIO (userError "dump cancellation was not delivered")
    bounded (readMVar killed) >>= either throwIO pure
    mapM diagnosticStat [0, 1] >>= (@?= [1, 1])

withOutput :: (FilePath -> Handle -> IO a) -> IO a
withOutput action = do
  directory <- getTemporaryDirectory
  bracket (openBinaryTempFile directory "rdkafka-diagnostic-") (\(path, handle) -> hClose handle `finally` removeFile path) (uncurry action)

readOutput :: Handle -> IO BS.ByteString
readOutput handle = do
  end <- hTell handle
  hSeek handle AbsoluteSeek 0
  result <- BS.hGet handle (fromIntegral end)
  hSeek handle AbsoluteSeek end
  pure result
