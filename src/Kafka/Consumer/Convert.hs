module Kafka.Consumer.Convert
  ( offsetSyncToInt,
    offsetToInt64,
    int64ToOffset,
    fromNativeTopicPartitionList'',
    fromNativeTopicPartitionList',
    fromNativeTopicPartitionList,
    toNativeTopicPartitionList,
    toNativeTopicPartitionListNoDispose,
    toNativeTopicPartitionList',
    topicPartitionFromMessage,
    topicPartitionFromMessageForCommit,
    toMap,
    fromMessagePtr,
    fromMessagePtrAttributed,
    offsetCommitToBool,
  )
where

import Control.Exception (finally)
import Control.Monad ((>=>))
import Data.Bifunctor (first)
import Data.ByteString qualified as BS
import Data.Either (fromRight)
import Data.Int (Int64)
import Data.Map.Strict (Map, fromListWith)
import Data.Set qualified as S
import Data.Text qualified as Text
import Foreign.C.Error (getErrno)
import Foreign.ForeignPtr (withForeignPtr)
import Foreign.Ptr (Ptr, nullPtr)
import Foreign.Storable (Storable (..))
import Kafka.Consumer.Types (ConsumerMessageError (..), ConsumerRecord (..), Offset (..), OffsetCommit (..), OffsetStoreSync (..), PartitionOffset (..), TopicPartition (..))
import Kafka.Internal.RdKafka
  ( RdKafkaMessageT (..),
    RdKafkaMessageTPtr,
    RdKafkaRespErrT (..),
    RdKafkaTopicPartitionListT (..),
    RdKafkaTopicPartitionListTPtr,
    RdKafkaTopicPartitionT (..),
    newRdKafkaTopicPartitionListT,
    peekCText,
    rdKafkaMessageDestroy,
    rdKafkaTopicPartitionListAdd,
    rdKafkaTopicPartitionListNew,
    rdKafkaTopicPartitionListSetOffset,
  )
import Kafka.Internal.Shared (kafkaRespErr, readHeaders, readKey, readPayload, readTimestamp, readTopic)
import Kafka.Types (KafkaError (..), PartitionId (..), TopicName (..))

-- | Converts offsets sync policy to integer (the way Kafka understands it):
--
--     * @OffsetSyncDisable == -1@
--
--     * @OffsetSyncImmediate == 0@
--
--     * @OffsetSyncInterval ms == ms@
offsetSyncToInt :: OffsetStoreSync -> Int
offsetSyncToInt sync =
  case sync of
    OffsetSyncDisable -> -1
    OffsetSyncImmediate -> 0
    OffsetSyncInterval ms -> ms
{-# INLINE offsetSyncToInt #-}

offsetToInt64 :: PartitionOffset -> Int64
offsetToInt64 o = case o of
  PartitionOffsetBeginning -> -2
  PartitionOffsetEnd -> -1
  PartitionOffset off -> off
  PartitionOffsetStored -> -1000
  PartitionOffsetInvalid -> -1001
{-# INLINE offsetToInt64 #-}

int64ToOffset :: Int64 -> PartitionOffset
int64ToOffset o
  | o == -2 = PartitionOffsetBeginning
  | o == -1 = PartitionOffsetEnd
  | o == -1000 = PartitionOffsetStored
  | o >= 0 = PartitionOffset o
  | otherwise = PartitionOffsetInvalid
{-# INLINE int64ToOffset #-}

fromNativeTopicPartitionList'' :: RdKafkaTopicPartitionListTPtr -> IO [TopicPartition]
fromNativeTopicPartitionList'' ptr =
  withForeignPtr ptr $ \fptr -> fromNativeTopicPartitionList' fptr

fromNativeTopicPartitionList' :: Ptr RdKafkaTopicPartitionListT -> IO [TopicPartition]
fromNativeTopicPartitionList' ppl =
  if ppl == nullPtr
    then return []
    else peek ppl >>= fromNativeTopicPartitionList

fromNativeTopicPartitionList :: RdKafkaTopicPartitionListT -> IO [TopicPartition]
fromNativeTopicPartitionList pl =
  let count = cnt'RdKafkaTopicPartitionListT pl
      elems = elems'RdKafkaTopicPartitionListT pl
   in mapM (peekElemOff elems >=> toPart) [0 .. (count - 1)]
  where
    toPart :: RdKafkaTopicPartitionT -> IO TopicPartition
    toPart p = do
      topic <- peekCText $ topic'RdKafkaTopicPartitionT p
      return
        TopicPartition
          { tpTopicName = TopicName topic,
            tpPartition = PartitionId $ partition'RdKafkaTopicPartitionT p,
            tpOffset = int64ToOffset $ offset'RdKafkaTopicPartitionT p
          }

toNativeTopicPartitionList :: [TopicPartition] -> IO RdKafkaTopicPartitionListTPtr
toNativeTopicPartitionList ps = do
  pl <- newRdKafkaTopicPartitionListT (length ps)
  mapM_
    ( \p -> do
        let TopicName tn = tpTopicName p
            (PartitionId tp) = tpPartition p
            to = offsetToInt64 $ tpOffset p
            tnS = Text.unpack tn
        _ <- rdKafkaTopicPartitionListAdd pl tnS tp
        rdKafkaTopicPartitionListSetOffset pl tnS tp to
    )
    ps
  return pl

toNativeTopicPartitionListNoDispose :: [TopicPartition] -> IO RdKafkaTopicPartitionListTPtr
toNativeTopicPartitionListNoDispose ps = do
  pl <- rdKafkaTopicPartitionListNew (length ps)
  mapM_
    ( \p -> do
        let TopicName tn = tpTopicName p
            (PartitionId tp) = tpPartition p
            to = offsetToInt64 $ tpOffset p
            tnS = Text.unpack tn
        _ <- rdKafkaTopicPartitionListAdd pl tnS tp
        rdKafkaTopicPartitionListSetOffset pl tnS tp to
    )
    ps
  return pl

toNativeTopicPartitionList' :: [(TopicName, PartitionId)] -> IO RdKafkaTopicPartitionListTPtr
toNativeTopicPartitionList' tps = do
  let utps = S.toList . S.fromList $ tps
  pl <- newRdKafkaTopicPartitionListT (length utps)
  mapM_ (\(TopicName t, PartitionId p) -> rdKafkaTopicPartitionListAdd pl (Text.unpack t) p) utps
  return pl

topicPartitionFromMessage :: ConsumerRecord k v -> TopicPartition
topicPartitionFromMessage m =
  let (Offset moff) = crOffset m
   in TopicPartition (crTopic m) (crPartition m) (PartitionOffset moff)

-- | Creates a topic partition message for use with the offset commit message.
-- We increment the offset by 1 here because when we commit, the offset is the position
-- the consumer reads from to process the next message.
topicPartitionFromMessageForCommit :: ConsumerRecord k v -> TopicPartition
topicPartitionFromMessageForCommit m =
  case topicPartitionFromMessage m of
    (TopicPartition t p (PartitionOffset moff)) -> TopicPartition t p (PartitionOffset $ moff + 1)
    other -> other

toMap :: (Ord k) => [(k, v)] -> Map k [v]
toMap kvs = fromListWith (++) [(k, [v]) | (k, v) <- kvs]

fromMessagePtr :: RdKafkaMessageTPtr -> IO (Either KafkaError (ConsumerRecord (Maybe BS.ByteString) (Maybe BS.ByteString)))
fromMessagePtr ptr = first cmeError <$> fromMessagePtrAttributed ptr

-- | Convert a polled message and destroy it, exactly once even if conversion
-- throws. A null pointer (a poll timeout) yields the errno-derived error with
-- no origin; an errored message keeps the topic, partition and offset
-- librdkafka set on it.
fromMessagePtrAttributed :: RdKafkaMessageTPtr -> IO (Either ConsumerMessageError (ConsumerRecord (Maybe BS.ByteString) (Maybe BS.ByteString)))
fromMessagePtrAttributed ptr =
  withForeignPtr ptr $ \realPtr ->
    if realPtr == nullPtr
      then (\err -> Left (ConsumerMessageError err Nothing Nothing Nothing)) . kafkaRespErr <$> getErrno
      else
        ( do
            s <- peek realPtr
            if err'RdKafkaMessageT s /= RdKafkaRespErrNoError
              then Left <$> attributed s
              else Right <$> mkRecord s realPtr
        )
          `finally` rdKafkaMessageDestroy realPtr
  where
    attributed msg = do
      topic <-
        if topic'RdKafkaMessageT msg == nullPtr
          then pure Nothing
          else Just . TopicName <$> readTopic msg
      let partition = partition'RdKafkaMessageT msg
          offset = offset'RdKafkaMessageT msg
      pure
        ConsumerMessageError
          { cmeError = KafkaResponseError (err'RdKafkaMessageT msg),
            cmeTopic = topic,
            cmePartition = if partition >= 0 then Just (PartitionId partition) else Nothing,
            cmeOffset = if offset >= 0 then Just (Offset offset) else Nothing
          }
    mkRecord msg rptr = do
      topic <- readTopic msg
      key <- readKey msg
      payload <- readPayload msg
      timestamp <- readTimestamp ptr
      headers <- fromRight mempty <$> readHeaders rptr
      return
        ConsumerRecord
          { crTopic = TopicName topic,
            crPartition = PartitionId $ partition'RdKafkaMessageT msg,
            crOffset = Offset $ offset'RdKafkaMessageT msg,
            crTimestamp = timestamp,
            crHeaders = headers,
            crKey = key,
            crValue = payload
          }

offsetCommitToBool :: OffsetCommit -> Bool
offsetCommitToBool OffsetCommit = False
offsetCommitToBool OffsetCommitAsync = True
