module Kafka.Integration.ConfigStringContractTest (tests) where

import Control.Exception (bracket, throwIO, try)
import Data.Map.Strict qualified as M
import Data.Text qualified as Text
import Kafka.Consumer qualified as C
import Kafka.Dump (dumpKafkaConf)
import Kafka.Producer qualified as P
import Kafka.TestEnv
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertBool, assertFailure, testCase, (@?=))

tests :: TestEnv -> TestTree
tests env =
  testGroup "config-strings" $
    [ testCase ("producer global rejects " <> label) $
        rejected (P.newProducer (P.brokersList (teBrokers env) <> P.extraProp key value)) releaseProducer
    | (label, key, value) <- globalInvalid
    ]
      <> [ testCase ("producer topic rejects " <> label) $
             rejected (P.newProducer (P.brokersList (teBrokers env) <> P.extraTopicProps (M.singleton key value))) releaseProducer
         | (label, key, value) <- [("NUL key", "message.timeout.ms\0ignored", "5000"), ("NUL value", "message.timeout.ms", "5000\0ignored")]
         ]
      <> [ testCase (label <> " consumer global rejects " <> field) $
             rejected (C.newAssignedConsumer (base label mode <> C.extraProp key value)) releaseConsumer
         | (label, mode) <- modes,
           (field, key, value) <- globalInvalid
         ]
      <> [ testCase (label <> " subscription rejects " <> field) $
             rejected (C.newConsumer (base label mode) (C.extraSubscriptionProps (M.singleton key value))) releaseConsumer
         | (label, mode) <- modes,
           (field, key, value) <- [("NUL key", "auto.offset.reset\0ignored", "earliest"), ("NUL value", "auto.offset.reset", "earliest\0ignored")]
         ]
      <> [ testCase "empty producer client id retains the native default" $
             bracket (P.newProducer (P.brokersList (teBrokers env) <> P.extraProp "client.id" "") >>= either throwIO pure) releaseProducer $ \producer -> do
               config <- dumpKafkaConf producer
               M.lookup "client.id" config @?= Just "rdkafka",
           testCase "consumer ordinary UTF8 values remain exact" $
             bracket (C.newAssignedConsumer (base "ordinary" C.CallbackPollModeSync <> C.extraProp "client.id" "配置-λ") >>= either throwIO pure) releaseConsumer $ \consumer -> do
               config <- dumpKafkaConf consumer
               M.lookup "client.id" config @?= Just "配置-λ",
           testCase "empty global key retains native unknown-key rejection" $ do
             result <- try @C.KafkaError (P.newProducer (P.brokersList (teBrokers env) <> P.extraProp "" "value"))
             case result of
               Left (C.KafkaUnknownConfigurationKey _) -> pure ()
               Left failure -> assertFailure (show failure)
               Right (Left failure) -> assertFailure (show failure)
               Right (Right producer) -> releaseProducer producer >> assertFailure "empty key was accepted"
         ]
  where
    base label mode = C.brokersList (teBrokers env) <> C.groupId (makeGroupId env ("config-string-" <> Text.pack label)) <> C.callbackPollMode mode
    modes = [("async", C.CallbackPollModeAsync), ("sync", C.CallbackPollModeSync)]
    globalInvalid = [("NUL key", "client.id\0ignored", "client"), ("NUL value", "client.id", "client\0ignored")]

rejected :: IO (Either C.KafkaError a) -> (a -> IO ()) -> IO ()
rejected action release = do
  result <- try @C.KafkaError action
  case result of
    Left (C.KafkaInvalidConfigurationValue message) -> assertBool (show message) ("NUL" `Text.isInfixOf` message)
    Left failure -> assertFailure (show failure)
    Right (Left failure) -> assertFailure (show failure)
    Right (Right client) -> release client >> assertFailure "configuration silently accepted a NUL prefix"
