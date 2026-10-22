{-# LANGUAGE BangPatterns #-}

module Kafka.Consumer.Callbacks
  ( rebalanceCallback,
    offsetCommitCallback,
    forwardPartitionsToMessageQueue,
    module X,
  )
where

import Control.Arrow ((&&&))
import Control.Monad (forM_, void)
import Foreign.ForeignPtr (finalizeForeignPtr, newForeignPtr_)
import Foreign.Ptr (nullPtr)
import Kafka.Callbacks as X
import Kafka.Consumer.Convert (fromNativeTopicPartitionList', fromNativeTopicPartitionList'')
import Kafka.Consumer.Types (KafkaConsumer (..), RebalanceEvent (..), TopicPartition (..))
import Kafka.Internal.RdKafka
import Kafka.Internal.Setup (Callback (..), HasKafka (..), HasKafkaConf (..), Kafka (..), KafkaConf (..), getRdMsgQueue)
import Kafka.Types (KafkaError (..), PartitionId (..), TopicName (..))

import Data.Text qualified as Text

-- | Sets a callback that is called when rebalance is needed.
rebalanceCallback :: (KafkaConsumer -> RebalanceEvent -> IO ()) -> Callback
rebalanceCallback callback =
  Callback $ \kc@(KafkaConf con _ _) -> rdKafkaConfSetRebalanceCb con (realCb kc)
  where
    realCb kc k err pl = do
      k' <- newForeignPtr_ k
      pls <- newForeignPtr_ pl
      setRebalanceCallback callback (KafkaConsumer (Kafka k') kc) (KafkaResponseError err) pls

-- | Sets a callback that is called when rebalance is needed.
--
-- The results of automatic or manual offset commits will be scheduled
-- for this callback and is served by 'Kafka.Consumer.pollMessage'.
--
-- If no partitions had valid offsets to commit this callback will be called
-- with 'KafkaResponseError' 'RdKafkaRespErrNoOffset' which is not to be considered
-- an error.
offsetCommitCallback :: (KafkaConsumer -> KafkaError -> [TopicPartition] -> IO ()) -> Callback
offsetCommitCallback callback =
  Callback $ \kc@(KafkaConf conf _ _) -> rdKafkaConfSetOffsetCommitCb conf (realCb kc)
  where
    realCb kc k err pl = do
      k' <- newForeignPtr_ k
      pls <- fromNativeTopicPartitionList' pl
      callback (KafkaConsumer (Kafka k') kc) (KafkaResponseError err) pls

-------------------------------------------------------------------------------

-- | In 'Kafka.Consumer.ConsumerProperties.CallbackPollModeAsync', forward each
-- partition's queue to the consumer's message queue, which 'Kafka.Consumer.pollMessage'
-- reads; without this, records stay on librdkafka's consumer queue, where the
-- background callback-poll loop discards them. Does nothing in sync mode.
--
-- The forwarding outlives the partition-queue reference, which is released
-- here rather than left to the garbage collector. Forward before assigning a
-- partition, or pause it around forwarding, so no record is fetched onto the
-- consumer queue in between.
forwardPartitionsToMessageQueue :: KafkaConsumer -> [(TopicName, PartitionId)] -> IO ()
forwardPartitionsToMessageQueue k ps = do
  mbq <- getRdMsgQueue (getKafkaConf k)
  forM_ mbq $ \mq -> forM_ ps $ \(t, p) -> redirectPartitionQueue (getKafka k) t p mq

redirectPartitionQueue :: Kafka -> TopicName -> PartitionId -> RdKafkaQueueTPtr -> IO ()
redirectPartitionQueue (Kafka k) (TopicName t) (PartitionId p) q = do
  mpq <- rdKafkaQueueGetPartition k (Text.unpack t) p
  case mpq of
    Nothing -> return ()
    Just pq -> rdKafkaQueueForward pq q >> finalizeForeignPtr pq

setRebalanceCallback ::
  (KafkaConsumer -> RebalanceEvent -> IO ()) ->
  KafkaConsumer ->
  KafkaError ->
  RdKafkaTopicPartitionListTPtr ->
  IO ()
setRebalanceCallback f k e pls = do
  ps <- fromNativeTopicPartitionList'' pls
  let assignment = (tpTopicName &&& tpPartition) <$> ps
  let (Kafka kptr) = getKafka k

  case e of
    KafkaResponseError RdKafkaRespErrAssignPartitions -> do
      f k (RebalanceBeforeAssign assignment)
      protocol <- rdKafkaRebalanceProtocol kptr
      if protocol == "COOPERATIVE"
        then void $ rdKafkaIncrementalAssign kptr pls
        else void $ rdKafkaAssign kptr pls

      mbq <- getRdMsgQueue $ getKafkaConf k
      case mbq of
        Nothing -> pure ()
        Just _ -> do
          {- Magnus Edenhill:
              If you redirect after assign() it means some messages may be forwarded to the single consumer queue,
              so either do it before assign() or do: assign(); pause(); redirect; resume()
          -}
          void $ rdKafkaPausePartitions kptr pls
          forwardPartitionsToMessageQueue k assignment
          void $ rdKafkaResumePartitions kptr pls

      f k (RebalanceAssign assignment)
    KafkaResponseError RdKafkaRespErrRevokePartitions -> do
      f k (RebalanceBeforeRevoke assignment)
      protocol <- rdKafkaRebalanceProtocol kptr
      if protocol == "COOPERATIVE"
        then void $ rdKafkaIncrementalUnassign kptr pls
        else void $ newForeignPtr_ nullPtr >>= rdKafkaAssign kptr
      f k (RebalanceRevoke assignment)
    x -> error $ "Rebalance: UNKNOWN response: " <> show x
