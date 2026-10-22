-- | 'pollMessageAttributed' reports which partition a partition-EOF belongs to
-- (improvement request IR-2, AC-2), in both callback poll modes.
module Kafka.Integration.AttributedErrorTest
  ( tests,
  )
where

import Control.Exception (finally)
import Control.Monad (void)
import Data.ByteString (ByteString)
import Data.Map qualified as Map
import Data.Text qualified as Text
import GHC.Clock (getMonotonicTime)
import Kafka.Consumer
import Kafka.TestEnv
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (Assertion, assertBool, assertFailure, testCase, (@?=))

tests :: TestEnv -> TestTree
tests env =
  testGroup
    "attributed"
    [ testCase "async" (scenario env CallbackPollModeAsync "async"),
      testCase "sync" (scenario env CallbackPollModeSync "sync")
    ]

type Record = ConsumerRecord (Maybe ByteString) (Maybe ByteString)

scenario :: TestEnv -> CallbackPollMode -> String -> Assertion
scenario env mode label =
  withTopic env (Text.pack ("attributed-" <> label)) 2 $ \topic producer -> do
    produceTo producer topic 1 "busy"
    let props =
          consumerProps env
            <> groupId (makeGroupId env (Text.pack ("attributed-" <> label)))
            <> noAutoCommit
            <> extraProps (Map.fromList [("enable.partition.eof", "true")])
            -- Last: the ConsumerProperties semigroup takes the right operand's
            -- poll mode.
            <> callbackPollMode mode
    consumer <- newAssignedConsumer props >>= either (assertFailure . ("newAssignedConsumer: " <>) . show) pure
    flip finally (void (closeConsumer consumer)) $ do
      assign
        consumer
        [ TopicPartition topic (PartitionId 0) PartitionOffsetBeginning,
          TopicPartition topic (PartitionId 1) PartitionOffsetBeginning
        ]
        >>= (@?= Nothing)

      (records, eofs) <- collectUntilEofs consumer 2
      fmap crValue records @?= [Just "busy"]
      fmap crPartition records @?= [PartitionId 1]

      let eof p o =
            ConsumerMessageError
              (KafkaResponseError RdKafkaRespErrPartitionEof)
              (Just topic)
              (Just (PartitionId p))
              (Just (Offset o))
      assertBool ("expected exactly one EOF per partition, got " <> show eofs) (length eofs == 2)
      assertBool ("partition 0 EOF at offset 0 missing from " <> show eofs) (eof 0 0 `elem` eofs)
      assertBool ("partition 1 EOF at offset 1 missing from " <> show eofs) (eof 1 1 `elem` eofs)

      -- A timeout belongs to no message.
      pollMessageAttributed consumer (Timeout 200) >>= \case
        Left err -> err @?= ConsumerMessageError (KafkaResponseError RdKafkaRespErrTimedOut) Nothing Nothing Nothing
        Right record -> assertFailure ("unexpected record " <> show record)

-- | Poll until @n@ partition-EOF errors have arrived (fifteen seconds), then
-- keep polling for two more seconds to catch duplicates. Timeouts are skipped;
-- any other error fails the test.
collectUntilEofs :: KafkaConsumer -> Int -> IO ([Record], [ConsumerMessageError])
collectUntilEofs consumer n = do
  start <- getMonotonicTime
  let go settleUntil records eofs = do
        now <- getMonotonicTime
        case settleUntil of
          Just t | now > t -> pure (reverse records, reverse eofs)
          Nothing | now > start + 15 -> assertFailure ("saw only " <> show (reverse eofs) <> " within 15 seconds")
          _ ->
            pollMessageAttributed consumer (Timeout 200) >>= \case
              Right record -> go settleUntil (record : records) eofs
              Left err
                | cmeError err == KafkaResponseError RdKafkaRespErrTimedOut -> go settleUntil records eofs
                | cmeError err == KafkaResponseError RdKafkaRespErrPartitionEof -> do
                    let eofs' = err : eofs
                        settle = if length eofs' >= n then Just (maybe (now + 2) id settleUntil) else settleUntil
                    go settle records eofs'
                | otherwise -> assertFailure ("unexpected error " <> show err)
  go Nothing [] []
