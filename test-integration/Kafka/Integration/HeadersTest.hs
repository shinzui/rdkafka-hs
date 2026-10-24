-- | Record headers survive the real broker path in wire order (IR-3): the
-- list a consumer decodes is exactly the list the producer sent, repeated
-- names and identical pairs included, through both polling APIs in both
-- callback modes.
module Kafka.Integration.HeadersTest
  ( tests,
  )
where

import Control.Exception (bracket, throwIO)
import Control.Monad (forM_)
import Data.Bifunctor (first)
import Data.ByteString (ByteString)
import Data.Text (Text)
import Data.Text.Encoding (encodeUtf8)
import GHC.Clock (getMonotonicTime)
import Kafka.Consumer
import Kafka.Producer (ProducePartition (..), ProducerRecord (..))
import Kafka.TestEnv
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (Assertion, assertFailure, testCase, (@?=))

-- | Which public polling function a case reads through.
data PollApi = PollMessage | PollMessageAttributed

data Fixture = Fixture
  { fxSlug :: Text,
    fxName :: String,
    fxHeaders :: [(ByteString, ByteString)],
    -- | header names and the value last-occurrence selection must return
    fxLast :: [(ByteString, Maybe ByteString)]
  }

tests :: TestEnv -> TestTree
tests env =
  testGroup
    "Headers wire order"
    [ testGroup
        (apiName <> " " <> modeName)
        [ testCase (fxName fx) (scenario env api mode (apiSlug <> "-" <> modeSlug) fx)
        | fx <- fixtures
        ]
    | (api, apiName, apiSlug) <- [(PollMessage, "pollMessage", "plain"), (PollMessageAttributed, "pollMessageAttributed", "attributed")],
      (mode, modeName, modeSlug) <- [(CallbackPollModeSync, "sync", "sync"), (CallbackPollModeAsync, "async", "async")]
    ]

fixtures :: [Fixture]
fixtures =
  [ Fixture
      "interleaved"
      "interleaved duplicate names keep wire order and last values"
      [("event-type", "Other"), ("account", "two"), ("account", "one"), ("event-type", "Happened")]
      [("event-type", Just "Happened"), ("account", Just "one"), ("absent", Nothing)],
    Fixture
      "distinct"
      "distinct names keep wire order"
      [("a-header", "1"), ("b-header", "2"), ("c-header", "3")]
      [("b-header", Just "2"), ("absent", Nothing)],
    Fixture
      "identical"
      "identical repeated pairs are not deduplicated"
      [("repeat", "same"), ("repeat", "same"), ("tail", "end")]
      [("repeat", Just "same"), ("tail", Just "end")],
    Fixture
      "single"
      "a single header round-trips"
      [("only", "value")]
      [("only", Just "value"), ("absent", Nothing)],
    Fixture
      "none"
      "no headers decode as an empty list"
      []
      [("absent", Nothing)]
  ]

-- | Produce one record carrying the fixture's headers to a fresh topic, read
-- it back through a fresh consumer group, and compare the complete list.
scenario :: TestEnv -> PollApi -> CallbackPollMode -> Text -> Fixture -> Assertion
scenario env api mode label fx =
  withTopic env suffix 1 $ \topic producer -> do
    let payload = encodeUtf8 suffix
    sendMessagesWithHeaders [ProducerRecord topic (SpecifiedPartition 0) Nothing (Just payload) mempty] (headersFromList (fxHeaders fx)) producer
      >>= either throwIO pure
    let props = consumerProps env <> groupId (makeGroupId env suffix) <> callbackPollMode mode
    bracket (mkConsumerFor topic props) releaseConsumer $ \consumer -> do
      record <- awaitRecord api consumer
      crTopic record @?= topic
      crPartition record @?= PartitionId 0
      crValue record @?= Just payload
      let received = headersToList (crHeaders record)
      received @?= fxHeaders fx
      forM_ (fxLast fx) $ \(name, expected) ->
        lastValue name received @?= expected
  where
    suffix = "headers-" <> label <> "-" <> fxSlug fx

-- | The value of the last header with the given name.
lastValue :: ByteString -> [(ByteString, ByteString)] -> Maybe ByteString
lastValue name = foldl' (\acc (n, v) -> if n == name then Just v else acc) Nothing

-- | The next record, retrying poll timeouts for at most ten seconds. Any other
-- error, or no record by the deadline, fails the case.
awaitRecord :: PollApi -> KafkaConsumer -> IO (ConsumerRecord (Maybe ByteString) (Maybe ByteString))
awaitRecord api consumer = do
  deadline <- (+ 10) <$> getMonotonicTime
  let go = do
        now <- getMonotonicTime
        if now > deadline
          then assertFailure "no record arrived within 10 seconds"
          else
            poll (Timeout 200) >>= \case
              Right record -> pure record
              Left (KafkaResponseError RdKafkaRespErrTimedOut) -> go
              Left err -> assertFailure ("poll failed: " <> show err)
  go
  where
    poll = case api of
      PollMessage -> pollMessage consumer
      PollMessageAttributed -> fmap (first cmeError) . pollMessageAttributed consumer
