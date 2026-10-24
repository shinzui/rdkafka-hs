module Kafka.Integration.SeekPartitionsTest (tests) where

import Control.Concurrent (forkFinally)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, readMVar)
import Control.Exception (SomeException, bracket, finally, throwIO, try)
import Control.Monad (forM_, void)
import Data.ByteString.Char8 qualified as BS
import Data.Either (isLeft)
import Data.List (sort)
import Data.Map.Strict qualified as M
import Data.Text qualified as Text
import GHC.Clock (getMonotonicTimeNSec)
import Kafka.Consumer
import Kafka.Metadata (WatermarkOffsets (..), watermarkOffsets)
import Kafka.SeekTestSupport
import Kafka.TestEnv
import System.Environment (lookupEnv)
import System.Exit (ExitCode (..))
import System.Process (readProcessWithExitCode)
import System.Timeout (timeout)
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertBool, assertFailure, testCase, (@?=))

tests :: TestEnv -> TestTree
tests env =
  testGroup
    "seek-partitions"
    [ testGroup label $
        [ testCase "unassigned member reports native error" $
            withClient env mode (label <> "-unassigned") $ \consumer ->
              seekPartitions consumer [TopicPartition (TopicName (prefixedName env "never-assigned")) (PartitionId 42) (PartitionOffset 0)] (Timeout 1000)
                >>= (@?= Just (KafkaResponseError RdKafkaRespErrUnknownPartition)),
          testCase "empty list preserves top-level invalid argument" $
            withClient env mode (label <> "-empty") $ \consumer ->
              seekPartitions consumer [] (Timeout 1000) >>= (@?= Just (KafkaResponseError RdKafkaRespErrInvalidArg)),
          testCase "zero-timeout unassigned failure is immediate" $
            withClient env mode (label <> "-zero-unknown") $ \consumer -> do
              start <- getMonotonicTimeNSec
              seekPartitions consumer [TopicPartition (TopicName (prefixedName env "never-assigned")) (PartitionId 42) (PartitionOffset 0)] (Timeout 0)
                >>= (@?= Just (KafkaResponseError RdKafkaRespErrUnknownPartition))
              elapsed <- subtract start <$> getMonotonicTimeNSec
              assertBool ("zero timeout took " <> show elapsed <> " ns") (elapsed < 1000000000),
          testCase "empty zero-timeout preserves top-level error" $
            withClient env mode (label <> "-zero-empty") $ \consumer ->
              seekPartitions consumer [] (Timeout 0) >>= (@?= Just (KafkaResponseError RdKafkaRespErrInvalidArg)),
          testCase "rewind without assignment does not seek" $
            withClient env mode (label <> "-no-assignment") $ \consumer -> withSeekFault 1 $ do
              rewindConsumer consumer (Timeout 1000) >>= (@?= Nothing)
              seekStat 1 >>= (@?= 0)
        ]
          <> boundaryTests env mode label
          <> brokerTests env mode label
    | (label, mode) <- [("async", CallbackPollModeAsync), ("sync", CallbackPollModeSync)]
    ]

withClient :: TestEnv -> CallbackPollMode -> String -> (KafkaConsumer -> IO a) -> IO a
withClient env mode label = bracket acquire releaseConsumer
  where
    acquire =
      newConsumer
        (consumerPropsNoStore env <> groupId (makeGroupId env (Text.pack ("seek-" <> label))) <> callbackPollMode mode)
        (offsetReset Earliest)
        >>= either (assertFailure . show) pure

boundaryTests :: TestEnv -> CallbackPollMode -> String -> [TestTree]
boundaryTests env mode label =
  [ probe "top-level error precedes member error and is destroyed" 3 1000 [42, 43] (Just RdKafkaRespErrTimedOut),
    probe "first substantive error follows input order" 5 1000 [42, 43] (Just RdKafkaRespErrInvalidArg),
    probe "reversed input selects the other substantive error" 5 1000 [43, 42] (Just RdKafkaRespErrUnknownPartition),
    probe "zero-timeout skips pending but reports later failure" 4 0 [42, 43] (Just RdKafkaRespErrUnknownPartition),
    probe "nonzero-timeout preserves residual pending" 4 1000 [42, 43] (Just RdKafkaRespErrInProgress),
    probe "zero-timeout all pending promises initiation only" 6 0 [42, 43] Nothing,
    probe "nonzero-timeout all pending is not success" 6 1000 [42, 43] (Just RdKafkaRespErrInProgress),
    testCase "top-level decoder exception still destroys error object" $
      withClient env mode (label <> "-decode") $ \consumer -> withSeekFault 7 $ do
        result <- try @SomeException $ seekPartitions consumer [TopicPartition (TopicName "controlled") (PartitionId 42) (PartitionOffset 0)] (Timeout 1000)
        assertBool (show result) (isLeft result)
        seekStat 2 >>= (@?= 1)
        seekStat 3 >>= (@?= 1),
    testCase "rewind assignment failure prevents seek" $
      withClient env mode (label <> "-assignment-error") $ \consumer -> withSeekFault 12 $ do
        rewindConsumer consumer (Timeout 1000) >>= (@?= Just (KafkaResponseError RdKafkaRespErrState))
        seekStat 1 >>= (@?= 0)
  ]
  where
    probe name fault milliseconds partitions expected = testCase name $
      withClient env mode (label <> "-" <> name) $ \consumer -> withSeekFault fault $ do
        seekPartitions consumer [TopicPartition (TopicName "controlled") (PartitionId p) (PartitionOffset 0) | p <- partitions] (Timeout milliseconds)
          >>= (@?= fmap KafkaResponseError expected)
        seekStat 1 >>= (@?= 1)
        seekStat 2 >>= (@?= if fault == 3 then 1 else 0)
        seekStat 3 >>= (@?= if fault == 3 then 1 else 0)

brokerTests :: TestEnv -> CallbackPollMode -> String -> [TestTree]
brokerTests env mode label =
  [ scenario "single seek replays exact record" $ \topic consumer -> do
      warm topic consumer
      seekPartitions consumer [part topic 0 (PartitionOffset 1)] (Timeout 2000) >>= (@?= Nothing)
      expectRecord consumer topic 0 1,
    scenario "multiple successful seeks replay both partitions" $ \topic consumer -> do
      assign consumer [part topic p (PartitionOffset 0) | p <- [0, 1]] >>= (@?= Nothing)
      records <- receiveMessages consumer 8 >>= either (assertFailure . show) pure
      sort [(crPartition r, crOffset r, crValue r) | r <- records]
        @?= sort [(PartitionId p, Offset (fromIntegral n), Just (payload p n)) | p <- [0, 1], n <- [0 .. 3]]
      seekPartitions consumer [part topic 0 (PartitionOffset 2), part topic 1 (PartitionOffset 1)] (Timeout 2000) >>= (@?= Nothing)
      replay <- receiveMessages consumer 5 >>= either (assertFailure . show) pure
      sort [(crPartition r, crOffset r, crValue r) | r <- replay]
        @?= sort [(PartitionId p, Offset (fromIntegral n), Just (payload p n)) | (p, ns) <- [(0, [2, 3]), (1, [1, 2, 3])], n <- ns],
    scenario "mixed seek reports failure without rolling back success" $ \topic consumer -> do
      warm topic consumer
      seekPartitions consumer [part topic 0 (PartitionOffset 1), part (TopicName (prefixedName env "seek-never-assigned")) 42 (PartitionOffset 0)] (Timeout 2000)
        >>= (@?= Just (KafkaResponseError RdKafkaRespErrUnknownPartition))
      expectRecord consumer topic 0 1,
    scenario "mixed errors follow actual request order" $ \topic consumer -> do
      warm topic consumer
      seekPartitions consumer [part topic 0 PartitionOffsetStored, part (TopicName (prefixedName env "seek-never-assigned")) 42 (PartitionOffset 0)] (Timeout 2000)
        >>= (@?= Just (KafkaResponseError RdKafkaRespErrInvalidArg))
      seekPartitions consumer [part (TopicName (prefixedName env "seek-never-assigned")) 42 (PartitionOffset 0), part topic 0 PartitionOffsetStored] (Timeout 2000)
        >>= (@?= Just (KafkaResponseError RdKafkaRespErrUnknownPartition)),
    scenario "zero-timeout valid seek is followed by synchronous replay" $ \topic consumer -> do
      warm topic consumer
      start <- getMonotonicTimeNSec
      seekPartitions consumer [part topic 0 (PartitionOffset 1)] (Timeout 0) >>= (@?= Nothing)
      elapsed <- subtract start <$> getMonotonicTimeNSec
      assertBool (show elapsed) (elapsed < 1000000000)
      -- Zero timeout promises no completion barrier. A synchronous seek supplies it.
      seekPartitions consumer [part topic 0 (PartitionOffset 1)] (Timeout 2000) >>= (@?= Nothing)
      expectRecord consumer topic 0 1,
    scenario "zero-timeout mixed request keeps immediate unknown error" $ \topic consumer -> do
      warm topic consumer
      seekPartitions consumer [part topic 0 (PartitionOffset 1), part (TopicName (prefixedName env "seek-never-assigned")) 42 (PartitionOffset 0)] (Timeout 0)
        >>= (@?= Just (KafkaResponseError RdKafkaRespErrUnknownPartition)),
    scenario "zero-timeout logical offsets promise initiation only" $ \topic consumer -> do
      warm topic consumer
      forM_ [PartitionOffsetBeginning, PartitionOffsetEnd, PartitionOffsetInvalid, PartitionOffsetStored, PartitionOffset (-2002)] $ \offset -> do
        seekPartitions consumer [part topic 0 offset] (Timeout 0) >>= (@?= Nothing)
        -- Even a later native rejection (STORED) cannot be reported by an
        -- initiation-only call. Synchronize with a supported absolute seek.
        seekPartitions consumer [part topic 0 (PartitionOffset 0)] (Timeout 2000) >>= (@?= Nothing)
        expectRecord consumer topic 0 0,
    scenario "infinite native timeout completes a valid seek" $ \topic consumer -> do
      warm topic consumer
      seekPartitions consumer [part topic 0 (PartitionOffset 2)] (Timeout (-1)) >>= (@?= Nothing)
      expectRecord consumer topic 0 2,
    scenario "duplicate entries preserve residual native pending error" $ \topic consumer -> do
      warm topic consumer
      -- Native replies update the first matching duplicate, leaving the second
      -- pending. Do not claim completion of both members or deduplicate silently.
      seekPartitions consumer [part topic 0 (PartitionOffset 1), part topic 0 (PartitionOffset 2)] (Timeout 2000)
        >>= (@?= Just (KafkaResponseError RdKafkaRespErrInProgress)),
    scenario "unassigned existing partition returns state error" $ \topic consumer -> do
      warm topic consumer
      assign consumer [] >>= (@?= Nothing)
      seekPartitions consumer [part topic 0 (PartitionOffset 0)] (Timeout 2000)
        >>= (@?= Just (KafkaResponseError RdKafkaRespErrState)),
    scenario "unknown topic reports member error" $ \_ consumer ->
      seekPartitions consumer [part (TopicName (prefixedName env "seek-unknown-topic")) 0 (PartitionOffset 0)] (Timeout 2000)
        >>= (@?= Just (KafkaResponseError RdKafkaRespErrUnknownPartition)),
    scenario "stored sentinel is rejected without altering position" $ \topic consumer -> do
      warm topic consumer
      seekPartitions consumer [part topic 0 PartitionOffsetStored] (Timeout 2000)
        >>= (@?= Just (KafkaResponseError RdKafkaRespErrInvalidArg))
      position consumer [(topic, PartitionId 0)] >>= (@?= Right [part topic 0 (PartitionOffset 4)]),
    scenario "end and exact high watermark produce no record" $ \topic consumer -> do
      warm topic consumer
      forM_ [PartitionOffsetEnd, PartitionOffset 4] $ \offset -> do
        seekPartitions consumer [part topic 0 offset] (Timeout 2000) >>= (@?= Nothing)
        pollMessage consumer (Timeout 250) >>= (@?= Left (KafkaResponseError RdKafkaRespErrTimedOut)),
    scenario "seek before retained log range resets to actual low watermark" $ \topic consumer -> do
      warm topic consumer
      trimPrefix topic
      -- Query the broker, rather than trusting the CLI's acknowledgement.
      offsets <- sequence <$> watermarkOffsets consumer (Timeout 5000) topic
      offsets @?= Right [WatermarkOffsets topic (PartitionId 0) (Offset 2) (Offset 4), WatermarkOffsets topic (PartitionId 1) (Offset 0) (Offset 4)]
      forM_ [PartitionOffset 0, PartitionOffsetBeginning, PartitionOffset 2] $ \offset -> do
        seekPartitions consumer [part topic 0 offset] (Timeout 2000) >>= (@?= Nothing)
        expectRecord consumer topic 0 2,
    scenario "rewind without committed offset starts at beginning" $ \topic consumer -> do
      warm topic consumer
      committed consumer (Timeout 5000) [(topic, PartitionId 0)] >>= (@?= Right [part topic 0 PartitionOffsetInvalid])
      rewindConsumer consumer (Timeout 5000) >>= (@?= Nothing)
      expectRecord consumer topic 0 0,
    scenario "rewind uses committed next offset exactly" $ \topic consumer -> do
      warm topic consumer
      commitPartitionsOffsets OffsetCommit consumer [part topic 0 (PartitionOffset 2)] >>= (@?= Nothing)
      committed consumer (Timeout 5000) [(topic, PartitionId 0)] >>= (@?= Right [part topic 0 (PartitionOffset 2)])
      rewindConsumer consumer (Timeout 5000) >>= (@?= Nothing)
      expectRecord consumer topic 0 2,
    scenario "rewind message commit resumes after committed message" $ \topic consumer -> do
      assign consumer [part topic 0 (PartitionOffset 0)] >>= (@?= Nothing)
      record <- nextRecord consumer
      crOffset record @?= Offset 0
      commitOffsetMessage OffsetCommit consumer record >>= (@?= Nothing)
      committed consumer (Timeout 5000) [(topic, PartitionId 0)] >>= (@?= Right [part topic 0 (PartitionOffset 1)])
      rewindConsumer consumer (Timeout 5000) >>= (@?= Nothing)
      expectRecord consumer topic 0 1,
    scenario "rewind lookup failure prevents seek" $ \topic consumer -> do
      warm topic consumer
      withSeekFault 11 $ do
        rewindConsumer consumer (Timeout 1000) >>= (@?= Just (KafkaResponseError RdKafkaRespErrTimedOut))
        seekStat 1 >>= (@?= 0),
    scenario "rewind propagates assignment change after committed lookup" $ \topic consumer -> do
      warm topic consumer
      commitPartitionsOffsets OffsetCommit consumer [part topic 0 (PartitionOffset 1)] >>= (@?= Nothing)
      withSeekFault 10 $ do
        result <- newEmptyMVar
        void $ forkFinally (rewindConsumer consumer (Timeout 5000)) (putMVar result)
        let join = timeout 10000000 (readMVar result) >>= maybe (assertFailure "rewind thread did not complete") (either throwIO pure)
        flip finally (releaseCommittedBoundary >> void join) $ do
          awaitCommittedBoundary
          assignment consumer >>= (@?= Right (M.singleton topic [PartitionId 0]))
          assign consumer [] >>= (@?= Nothing)
          assignment consumer >>= (@?= Right M.empty)
          releaseCommittedBoundary
          join >>= (@?= Just (KafkaResponseError RdKafkaRespErrState))
          seekStat 1 >>= (@?= 1)
  ]
    <> [ scenario name $ \topic consumer -> do
           warm topic consumer
           seekPartitions consumer [part topic 0 offset] (Timeout 2000) >>= (@?= Nothing)
           expectRecord consumer topic 0 expected
       | (name, offset, expected) <-
           [ ("beginning sentinel replays offset zero", PartitionOffsetBeginning, 0),
             ("absolute low watermark replays offset zero", PartitionOffset 0, 0),
             ("invalid sentinel uses configured earliest reset", PartitionOffsetInvalid, 0),
             ("above high watermark uses configured earliest reset", PartitionOffset 999999, 0),
             ("native tail sentinel replays final two records", PartitionOffset (-2002), 2)
           ]
       ]
  where
    scenario name action = testCase name $
      withTopic env (Text.pack ("seek-" <> label <> "-" <> take 48 (map slug name))) 2 $ \topic producer -> do
        forM_ [0, 1] $ \p -> forM_ [0 .. 3] $ \n -> produceTo producer topic p (payload p n)
        withClient env mode (label <> "-" <> map slug name) (action topic)
    slug c
      | c >= 'a' && c <= 'z' = c
      | otherwise = '-'

part :: TopicName -> Int -> PartitionOffset -> TopicPartition
part topic partition = TopicPartition topic (PartitionId partition)

payload :: Int -> Int -> BS.ByteString
payload partition offset = BS.pack ("partition=" <> show partition <> ";offset=" <> show offset)

warm :: TopicName -> KafkaConsumer -> IO ()
warm topic consumer = do
  assign consumer [part topic 0 (PartitionOffset 0)] >>= (@?= Nothing)
  forM_ [0 .. 3] (expectRecord consumer topic 0)

nextRecord :: KafkaConsumer -> IO (ConsumerRecord (Maybe BS.ByteString) (Maybe BS.ByteString))
nextRecord consumer = pollMessage consumer (Timeout 10000) >>= either (assertFailure . show) pure

expectRecord :: KafkaConsumer -> TopicName -> Int -> Int -> IO ()
expectRecord consumer topic partition offset = do
  record <- nextRecord consumer
  (crTopic record, crPartition record, crOffset record, crKey record, crValue record, crHeaders record)
    @?= (topic, PartitionId partition, Offset (fromIntegral offset), Nothing, Just (payload partition offset), mempty)

trimPrefix :: TopicName -> IO ()
trimPrefix (TopicName topic) = do
  configured <- lookupEnv "RDKAFKA_TEST_RPK"
  let args = ["topic", "trim-prefix", Text.unpack topic, "--offset", "2", "--partitions", "0", "--no-confirm"]
      (command, commandArgs) = case configured of
        Just path -> (path, args)
        Nothing -> ("container", ["exec", "rdkafka-hs-rp-0", "rpk"] <> args)
  (exit, output, errors) <- readProcessWithExitCode command commandArgs ""
  exit @?= ExitSuccess
  assertBool (errors <> output) (not (null output))
