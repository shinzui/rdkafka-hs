-- | Watermark queries honour the consumer's @isolation.level@ (improvement
-- request IR-2, AC-4): on a @read_committed@ consumer the high watermark is the
-- last stable offset, which stops before an open transaction.
module Kafka.Integration.ReadCommittedTest
  ( tests,
  )
where

import Control.Exception (finally)
import Data.Map qualified as Map
import Data.Text (Text)
import GHC.Clock (getMonotonicTime)
import Kafka.Consumer
import Kafka.Metadata (WatermarkOffsets (..), partitionWatermarkOffsets)
import Kafka.Producer qualified as P
import Kafka.TestEnv
import Kafka.Transaction (beginTransaction, commitTransaction, initTransactions)
import Test.Tasty (TestTree)
import Test.Tasty.HUnit (assertBool, assertFailure, testCase, (@?=))

tests :: TestEnv -> TestTree
tests env =
  testCase "read_committed watermark stops before an open transaction" $
    withTopic env "read-committed" 1 $ \topic _ -> do
      producer <-
        P.newProducer
          (producerProps env <> P.extraProps (Map.fromList [("transactional.id", prefixedName env "read-committed-tx")]))
          >>= either (assertFailure . ("newProducer: " <>) . show) pure
      flip finally (P.closeProducer producer) $ do
        initTransactions producer (Timeout 10000) >>= (@?= Nothing)
        beginTransaction producer >>= (@?= Nothing)
        P.produceMessage producer (P.ProducerRecord topic (P.SpecifiedPartition 0) Nothing (Just "pending") mempty)
          >>= (@?= Nothing)
        P.flushProducer producer

        readCommitted <- observer topic "read_committed"
        readUncommitted <- observer topic "read_uncommitted"
        flip finally (destroyConsumer readCommitted >> destroyConsumer readUncommitted) $ do
          openRc <- high readCommitted topic
          openRu <- high readUncommitted topic
          assertBool
            ("while the transaction is open, read_committed high " <> show openRc <> " must be below read_uncommitted high " <> show openRu)
            (openRc < openRu)

          commitTransaction producer (Timeout 10000) >>= assertBool "commitTransaction failed" . null
          deadline <- (+ 10) <$> getMonotonicTime
          let converge = do
                rc <- high readCommitted topic
                ru <- high readUncommitted topic
                now <- getMonotonicTime
                if rc == ru
                  then assertBool ("read_committed high " <> show rc <> " did not advance past " <> show openRc) (rc > openRc)
                  else
                    if now > deadline
                      then assertFailure ("after commit, read_committed high " <> show rc <> " never reached " <> show ru)
                      else converge
          converge
  where
    observer topic (level :: Text) = do
      consumer <-
        newAssignedConsumer
          ( consumerProps env
              <> groupId (makeGroupId env ("read-committed-" <> level))
              <> noAutoCommit
              <> extraProps (Map.fromList [("isolation.level", level)])
          )
          >>= either (assertFailure . ("newAssignedConsumer: " <>) . show) pure
      assign consumer [TopicPartition topic (PartitionId 0) PartitionOffsetBeginning] >>= (@?= Nothing)
      pure consumer
    high consumer topic =
      partitionWatermarkOffsets consumer (Timeout 5000) topic (PartitionId 0)
        >>= either (assertFailure . ("partitionWatermarkOffsets: " <>) . show) (pure . woHighWatermark)
