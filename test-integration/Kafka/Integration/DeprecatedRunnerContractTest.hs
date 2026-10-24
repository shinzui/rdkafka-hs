{-# OPTIONS_GHC -Wno-deprecations #-}

module Kafka.Integration.DeprecatedRunnerContractTest (tests) where

import Control.Concurrent.MVar (newEmptyMVar, tryPutMVar, tryTakeMVar)
import Control.Exception (Exception, finally, throwIO, try)
import Control.Monad (void)
import Data.IORef (newIORef, readIORef, writeIORef)
import Data.Map.Strict qualified as M
import Data.Text qualified as Text
import Kafka.Consumer qualified as C
import Kafka.Consumer.Types qualified as CT
import Kafka.Producer qualified as P
import Kafka.Producer.Types qualified as PT
import Kafka.TestEnv
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertBool, assertFailure, testCase, (@?=))

data Outcome = Returned | Refused | Thrown deriving (Eq, Show)

data HandlerFailure = HandlerFailure deriving (Eq, Show)

instance Exception HandlerFailure

marker :: C.KafkaError
marker = C.KafkaBadSpecification "runner-handler-result"

finish :: Outcome -> IO (Either C.KafkaError Int)
finish Returned = pure (Right 42)
finish Refused = pure (Left marker)
finish Thrown = throwIO HandlerFailure

checkResult :: Outcome -> Either HandlerFailure (Either C.KafkaError Int) -> IO ()
checkResult Returned result = result @?= Right (Right 42)
checkResult Refused result = result @?= Right (Left marker)
checkResult Thrown result = result @?= Left HandlerFailure

tests :: TestEnv -> TestTree
tests env =
  testGroup "deprecated-runners" $
    [ testCase ("producer flushes after " <> show outcome) $
        withTopic env (Text.pack ("runner-producer-" <> show outcome)) 1 $ \topic _ -> do
          retained <- newIORef Nothing
          reports <- newEmptyMVar
          let props = P.brokersList (teBrokers env) <> P.sendTimeout (C.Timeout 5000) <> P.setCallback (P.deliveryCallback (void . tryPutMVar reports))
              cleanup = readIORef retained >>= mapM_ releaseProducer
          flip finally cleanup $ do
            result <- try @HandlerFailure $ P.runProducer props $ \producer -> do
              let alias = PT.KafkaProducer (PT.kpKafkaPtr producer) (PT.kpKafkaConf producer) (PT.kpTopicConf producer)
              writeIORef retained (Just alias)
              P.produceMessage alias (P.ProducerRecord topic (P.SpecifiedPartition 0) (Just "runner-key") (Just "runner-value") mempty) >>= (@?= Nothing)
              finish outcome
            checkResult outcome result
            tryTakeMVar reports >>= \case
              Just (P.DeliverySuccess record offset) -> (P.prTopic record, P.prKey record, P.prValue record, offset) @?= (topic, Just "runner-key", Just "runner-value", C.Offset 0)
              other -> assertFailure ("wrapper did not flush its delivery report: " <> show other)
    | outcome <- [Returned, Refused, Thrown]
    ]
      <> [ testCase (label <> " consumer closes after " <> show outcome) $
             withTopic env (Text.pack ("runner-consumer-" <> label <> "-" <> show outcome)) 1 $ \topic producer -> do
               produceTo producer topic 0 "runner-record"
               retained <- newIORef Nothing
               let props = C.brokersList (teBrokers env) <> C.groupId (makeGroupId env (Text.pack ("runner-" <> label <> "-" <> show outcome))) <> C.noAutoCommit <> C.callbackPollMode mode
                   cleanup = readIORef retained >>= mapM_ releaseConsumer
               flip finally cleanup $ do
                 result <- try @HandlerFailure $ C.runConsumer props (C.topics [topic] <> C.offsetReset C.Earliest) $ \consumer -> do
                   let alias = CT.KafkaConsumer (CT.kcKafkaPtr consumer) (CT.kcKafkaConf consumer)
                   writeIORef retained (Just alias)
                   C.consumerFatalError alias >>= (@?= Nothing)
                   message <- C.pollMessage alias (C.Timeout 10000) >>= either throwIO pure
                   (C.crTopic message, C.crPartition message, C.crOffset message, C.crValue message) @?= (topic, C.PartitionId 0, C.Offset 0, Just "runner-record")
                   finish outcome
                 checkResult outcome result
                 readIORef retained >>= maybe (assertFailure "runner did not invoke handler") (\consumer -> C.consumerClosed consumer >>= (@?= True))
         | (label, mode) <- [("async", C.CallbackPollModeAsync), ("sync", C.CallbackPollModeSync)],
           outcome <- [Returned, Refused, Thrown]
         ]
      <> [ testCase "producer preserves native construction failure without invoking handler" $ do
             called <- newIORef False
             result <- P.runProducer (P.brokersList (teBrokers env) <> P.extraProps sasl) (\_ -> writeIORef called True >> pure (Right (42 :: Int)))
             nativeFailure result
             readIORef called >>= (@?= False),
           testCase "producer preserves configuration exceptions without invoking handler" $ do
             called <- newIORef False
             result <- try @C.KafkaError $ P.runProducer (P.extraProp "not.a.kafka.property" "x") (\_ -> writeIORef called True >> pure (Right (42 :: Int)))
             unknownConfiguration result
             readIORef called >>= (@?= False)
         ]
      <> [ testCase (label <> " consumer preserves " <> kind <> " construction failure") $ do
             called <- newIORef False
             let base = C.brokersList (teBrokers env) <> C.groupId (makeGroupId env (Text.pack ("runner-failure-" <> label <> "-" <> kind))) <> C.callbackPollMode mode
             if kind == "native"
               then C.runConsumer (base <> C.extraProps sasl) mempty (\_ -> writeIORef called True >> pure (Right (42 :: Int))) >>= nativeFailure
               else (try @C.KafkaError $ C.runConsumer (base <> C.extraProp "not.a.kafka.property" "x") mempty (\_ -> writeIORef called True >> pure (Right (42 :: Int)))) >>= unknownConfiguration
             readIORef called >>= (@?= False)
         | (label, mode) <- [("async", C.CallbackPollModeAsync), ("sync", C.CallbackPollModeSync)],
           kind <- ["native", "configuration"]
         ]

sasl :: M.Map Text.Text Text.Text
sasl = M.fromList [("security.protocol", "SASL_PLAINTEXT"), ("sasl.mechanisms", "PLAIN")]

nativeFailure :: Either C.KafkaError Int -> IO ()
nativeFailure (Left (C.KafkaError message)) = assertBool (show message) ("sasl.username" `Text.isInfixOf` message)
nativeFailure other = assertFailure (show other)

unknownConfiguration :: Either C.KafkaError (Either C.KafkaError Int) -> IO ()
unknownConfiguration (Left (C.KafkaUnknownConfigurationKey _)) = pure ()
unknownConfiguration other = assertFailure (show other)
