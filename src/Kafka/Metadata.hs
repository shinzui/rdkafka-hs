{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE OverloadedStrings #-}

-----------------------------------------------------------------------------

-----------------------------------------------------------------------------

-- |
-- Module with metadata types and functions.
module Kafka.Metadata
  ( KafkaMetadata (..),
    BrokerMetadata (..),
    TopicMetadata (..),
    PartitionMetadata (..),
    WatermarkOffsets (..),
    GroupMemberId (..),
    GroupMemberInfo (..),
    GroupProtocolType (..),
    GroupProtocol (..),
    GroupState (..),
    GroupInfo (..),
    allTopicsMetadata,
    topicMetadata,
    watermarkOffsets,
    watermarkOffsets',
    partitionWatermarkOffsets,
    offsetsForTime,
    offsetsForTime',
    topicOffsetsForTime,
    allConsumerGroupsInfo,
    consumerGroupInfo,
  )
where

import Control.Exception (bracket)
import Control.Monad.IO.Class (MonadIO (liftIO))
import Data.Bifunctor (bimap)
import Data.ByteString (ByteString, pack)
import Data.List (find)
import Data.Maybe (listToMaybe, mapMaybe)
import Data.Text (Text)
import Foreign (ForeignPtr, Storable (peek), finalizeForeignPtr, peekArray, withForeignPtr)
import GHC.Generics (Generic)
import Kafka.Consumer.Convert (fromNativeTopicPartitionList'', nativeTopicPartitionErrors, toNativeTopicPartitionList)
import Kafka.Consumer.Types (ConsumerGroupId (..), Offset (..), PartitionOffset (..), TopicPartition (..))
import Kafka.Internal.RdKafka (RdKafkaGroupInfoT (..), RdKafkaGroupListT (..), RdKafkaGroupListTPtr, RdKafkaGroupMemberInfoT (..), RdKafkaMetadataBrokerT (..), RdKafkaMetadataPartitionT (..), RdKafkaMetadataT (..), RdKafkaMetadataTPtr, RdKafkaMetadataTopicT (..), RdKafkaRespErrT (..), RdKafkaTPtr, destroyUnmanagedRdKafkaTopic, newUnmanagedRdKafkaTopicT, peekCText, rdKafkaListGroups, rdKafkaMetadata, rdKafkaOffsetsForTimes, rdKafkaQueryWatermarkOffsets)
import Kafka.Internal.Setup (HasKafka (..), Kafka (..))
import Kafka.Internal.Shared (kafkaErrorToMaybe)
import Kafka.Types (BrokerId (..), ClientId (..), KafkaError (..), Millis (..), PartitionId (..), Timeout (..), TopicName (..))

import Data.Set qualified as S
import Data.Text qualified as Text

data KafkaMetadata = KafkaMetadata
  { kmBrokers :: [BrokerMetadata],
    kmTopics :: [TopicMetadata],
    kmOrigBroker :: !BrokerId
  }
  deriving stock (Show, Eq, Generic)

data BrokerMetadata = BrokerMetadata
  { bmBrokerId :: !BrokerId,
    bmBrokerHost :: !Text,
    bmBrokerPort :: !Int
  }
  deriving stock (Show, Eq, Generic)

data PartitionMetadata = PartitionMetadata
  { pmPartitionId :: !PartitionId,
    pmError :: Maybe KafkaError,
    pmLeader :: !BrokerId,
    pmReplicas :: [BrokerId],
    pmInSyncReplicas :: [BrokerId]
  }
  deriving stock (Show, Eq, Generic)

data TopicMetadata = TopicMetadata
  { tmTopicName :: !TopicName,
    tmPartitions :: [PartitionMetadata],
    tmError :: Maybe KafkaError
  }
  deriving stock (Show, Eq, Generic)

data WatermarkOffsets = WatermarkOffsets
  { woTopicName :: !TopicName,
    woPartitionId :: !PartitionId,
    woLowWatermark :: !Offset,
    woHighWatermark :: !Offset
  }
  deriving stock (Show, Eq, Generic)

newtype GroupMemberId = GroupMemberId Text
  deriving stock (Show, Eq, Read, Ord)

data GroupMemberInfo = GroupMemberInfo
  { gmiMemberId :: !GroupMemberId,
    gmiClientId :: !ClientId,
    gmiClientHost :: !Text,
    gmiMetadata :: !ByteString,
    gmiAssignment :: !ByteString
  }
  deriving stock (Show, Eq, Generic)

newtype GroupProtocolType = GroupProtocolType Text
  deriving stock (Show, Eq, Read, Ord, Generic)

newtype GroupProtocol = GroupProtocol Text
  deriving stock (Show, Eq, Read, Ord, Generic)

data GroupState
  = -- | Group is preparing to rebalance
    GroupPreparingRebalance
  | -- | Group has no more members, but lingers until all offsets have expired
    GroupEmpty
  | -- | Group is awaiting state assignment from the leader
    GroupAwaitingSync
  | -- | Group is stable
    GroupStable
  | -- | Group has no more members and its metadata is being removed
    GroupDead
  deriving stock (Show, Eq, Read, Ord, Generic)

data GroupInfo = GroupInfo
  { giGroup :: !ConsumerGroupId,
    giError :: Maybe KafkaError,
    giState :: !GroupState,
    giProtocolType :: !GroupProtocolType,
    giProtocol :: !GroupProtocol,
    giMembers :: [GroupMemberInfo]
  }
  deriving stock (Show, Eq, Generic)

-- | Returns metadata for all topics in the cluster
allTopicsMetadata :: (MonadIO m, HasKafka k) => k -> Timeout -> m (Either KafkaError KafkaMetadata)
allTopicsMetadata k (Timeout timeout) =
  liftIO $
    withNativeResult k (rdKafkaMetadata (getKafkaPtr k) True Nothing timeout) (fmap Right . fromKafkaMetadataPtr)

-- | Returns metadata only for specified topic
topicMetadata :: (MonadIO m, HasKafka k) => k -> Timeout -> TopicName -> m (Either KafkaError KafkaMetadata)
topicMetadata k (Timeout timeout) (TopicName tn) = liftIO $ withForeignPtr (getKafkaPtr k) $ \_ ->
  bracket mkTopic clTopic $ \mbt -> case mbt of
    Left err -> return (Left $ KafkaError (Text.pack err))
    Right t -> do
      withNativeResult k (rdKafkaMetadata (getKafkaPtr k) False (Just t) timeout) (fmap Right . fromKafkaMetadataPtr)
  where
    mkTopic = newUnmanagedRdKafkaTopicT (getKafkaPtr k) (Text.unpack tn) Nothing
    clTopic = either (return . const ()) destroyUnmanagedRdKafkaTopic

-- | Query broker for low (oldest/beginning) and high (newest/end) offsets for a given topic.
watermarkOffsets :: (MonadIO m, HasKafka k) => k -> Timeout -> TopicName -> m [Either KafkaError WatermarkOffsets]
watermarkOffsets k timeout t = do
  meta <- topicMetadata k timeout t
  case meta of
    Left err -> return [Left err]
    Right tm -> case kmTopics tm of
      [] -> return []
      (t' : _) -> watermarkOffsets' k timeout t'

-- | Query broker for low (oldest/beginning) and high (newest/end) offsets for a given topic.
watermarkOffsets' :: (MonadIO m, HasKafka k) => k -> Timeout -> TopicMetadata -> m [Either KafkaError WatermarkOffsets]
watermarkOffsets' k timeout tm = case tmError tm of
  Just failure -> pure [Left failure]
  Nothing -> liftIO $ traverse query (tmPartitions tm)
    where
      query partition = case pmError partition of
        Just failure -> pure (Left failure)
        Nothing -> partitionWatermarkOffsets k timeout (tmTopicName tm) (pmPartitionId partition)

-- | Query broker for low (oldest/beginning) and high (newest/end) offsets for a specific partition
partitionWatermarkOffsets :: (MonadIO m, HasKafka k) => k -> Timeout -> TopicName -> PartitionId -> m (Either KafkaError WatermarkOffsets)
partitionWatermarkOffsets k (Timeout timeout) (TopicName t) (PartitionId p) = liftIO $ do
  offs <- rdKafkaQueryWatermarkOffsets (getKafkaPtr k) (Text.unpack t) p timeout
  return $ bimap KafkaResponseError toWatermark offs
  where
    toWatermark (l, h) = WatermarkOffsets (TopicName t) (PartitionId p) (Offset l) (Offset h)

-- | Look up the offsets for the given topic by timestamp.
--
-- The returned offset for each partition is the earliest offset whose
-- timestamp is greater than or equal to the given timestamp in the
-- corresponding partition.
topicOffsetsForTime :: (MonadIO m, HasKafka k) => k -> Timeout -> Millis -> TopicName -> m (Either KafkaError [TopicPartition])
topicOffsetsForTime k timeout timestamp topic = do
  meta <- topicMetadata k timeout topic
  case meta of
    Left err -> return $ Left err
    Right meta' -> case listToMaybe (mapMaybe tmError (kmTopics meta') <> concatMap (mapMaybe pmError . tmPartitions) (kmTopics meta')) of
      Just failure -> pure (Left failure)
      Nothing ->
        let tps = [(tmTopicName t, pmPartitionId p) | t <- kmTopics meta', p <- tmPartitions t]
         in offsetsForTime k timeout timestamp tps

-- | Look up the offsets for the given metadata by timestamp.
--
-- The returned offset for each partition is the earliest offset whose
-- timestamp is greater than or equal to the given timestamp in the
-- corresponding partition.
offsetsForTime' :: (MonadIO m, HasKafka k) => k -> Timeout -> Millis -> TopicMetadata -> m (Either KafkaError [TopicPartition])
offsetsForTime' k timeout timestamp t = case tmError t of
  Just failure -> pure (Left failure)
  Nothing -> case listToMaybe (mapMaybe pmError (tmPartitions t)) of
    Just failure -> pure (Left failure)
    Nothing ->
      let tps = [(tmTopicName t, pmPartitionId p) | p <- tmPartitions t]
       in offsetsForTime k timeout timestamp tps

-- | Look up the offsets for the given partitions by timestamp.
--
-- The returned offset for each partition is the earliest offset whose
-- timestamp is greater than or equal to the given timestamp in the
-- corresponding partition.
offsetsForTime :: (MonadIO m, HasKafka k) => k -> Timeout -> Millis -> [(TopicName, PartitionId)] -> m (Either KafkaError [TopicPartition])
offsetsForTime k (Timeout timeout) (Millis t) tps = liftIO $ withForeignPtr (getKafkaPtr k) $ \_ -> bracket (toNativeTopicPartitionList (mkTopicPartition <$> uniqueTps)) finalizeForeignPtr $ \ntps -> do
  res <- rdKafkaOffsetsForTimes (getKafkaPtr k) ntps timeout
  case res of
    RdKafkaRespErrNoError -> do
      errors <- nativeTopicPartitionErrors ntps
      case find (/= RdKafkaRespErrNoError) errors of
        Just failure -> pure (Left (KafkaResponseError failure))
        Nothing -> Right <$> fromNativeTopicPartitionList'' ntps
    err -> return $ Left (KafkaResponseError err)
  where
    uniqueTps = S.toList . S.fromList $ tps
    -- rd_kafka_offsets_for_times reuses `offset` to specify timestamp :(
    mkTopicPartition (tn, p) = TopicPartition tn p (PartitionOffset t)

-- | List and describe all consumer groups in cluster. Unsupported native
-- group-state names return a descriptive error.
allConsumerGroupsInfo :: (MonadIO m, HasKafka k) => k -> Timeout -> m (Either KafkaError [GroupInfo])
allConsumerGroupsInfo k (Timeout t) =
  liftIO $
    withNativeResult k (rdKafkaListGroups (getKafkaPtr k) Nothing t) fromGroupInfoListPtr

-- | Describe a given consumer group. Unsupported native group-state names
-- return a descriptive error; a missing group returns an empty list.
consumerGroupInfo :: (MonadIO m, HasKafka k) => k -> Timeout -> ConsumerGroupId -> m (Either KafkaError [GroupInfo])
consumerGroupInfo k (Timeout timeout) (ConsumerGroupId gn) =
  liftIO $
    withNativeResult k (rdKafkaListGroups (getKafkaPtr k) (Just (Text.unpack gn)) timeout) fromGroupInfoListPtr

-------------------------------------------------------------------------------
getKafkaPtr :: (HasKafka k) => k -> RdKafkaTPtr
getKafkaPtr k = let (Kafka k') = getKafka k in k'
{-# INLINE getKafkaPtr #-}

-- Retain the client until its complete result has been copied and destroyed.
-- Mask acquisition through ForeignPtr registration, including native return.
withNativeResult :: (HasKafka k) => k -> IO (Either RdKafkaRespErrT (ForeignPtr a)) -> (ForeignPtr a -> IO (Either KafkaError b)) -> IO (Either KafkaError b)
withNativeResult client acquire decode = withForeignPtr (getKafkaPtr client) $ \_ ->
  bracket acquire (either (const (pure ())) finalizeForeignPtr) $
    either (pure . Left . KafkaResponseError) decode

fromGroupInfoListPtr :: RdKafkaGroupListTPtr -> IO (Either KafkaError [GroupInfo])
fromGroupInfoListPtr ptr =
  withForeignPtr ptr $ \realPtr -> do
    gl <- peek realPtr
    pgis <- peekArray (groupCnt'RdKafkaGroupListT gl) (groups'RdKafkaGroupListT gl)
    sequence <$> traverse fromGroupInfoPtr pgis

fromGroupInfoPtr :: RdKafkaGroupInfoT -> IO (Either KafkaError GroupInfo)
fromGroupInfoPtr gi = do
  -- bmd <- peek (broker'RdKafkaGroupInfoT gi) -- >>= fromBrokerMetadataPtr
  -- xxx <- fromBrokerMetadataPtr bmd
  cid <- peekCText $ group'RdKafkaGroupInfoT gi
  stt <- peekCText $ state'RdKafkaGroupInfoT gi
  prt <- peekCText $ protocolType'RdKafkaGroupInfoT gi
  pr <- peekCText $ protocol'RdKafkaGroupInfoT gi
  mbs <- peekArray (memberCnt'RdKafkaGroupInfoT gi) (members'RdKafkaGroupInfoT gi)
  mbl <- mapM fromGroupMemberInfoPtr mbs
  pure $ do
    state <- groupStateFromKafkaString stt
    pure
      GroupInfo
        { -- giBroker = bmd
          giGroup = ConsumerGroupId cid,
          giError = kafkaErrorToMaybe $ KafkaResponseError (err'RdKafkaGroupInfoT gi),
          giState = state,
          giProtocolType = GroupProtocolType prt,
          giProtocol = GroupProtocol pr,
          giMembers = mbl
        }

fromGroupMemberInfoPtr :: RdKafkaGroupMemberInfoT -> IO GroupMemberInfo
fromGroupMemberInfoPtr mi = do
  mid <- peekCText $ memberId'RdKafkaGroupMemberInfoT mi
  cid <- peekCText $ clientId'RdKafkaGroupMemberInfoT mi
  hst <- peekCText $ clientHost'RdKafkaGroupMemberInfoT mi
  mtd <- peekArray (memberMetadataSize'RdKafkaGroupMemberInfoT mi) (memberMetadata'RdKafkaGroupMemberInfoT mi)
  ass <- peekArray (memberAssignmentSize'RdKafkaGroupMemberInfoT mi) (memberAssignment'RdKafkaGroupMemberInfoT mi)
  return
    GroupMemberInfo
      { gmiMemberId = GroupMemberId mid,
        gmiClientId = ClientId cid,
        gmiClientHost = hst,
        gmiMetadata = pack mtd,
        gmiAssignment = pack ass
      }

fromTopicMetadataPtr :: RdKafkaMetadataTopicT -> IO TopicMetadata
fromTopicMetadataPtr tm = do
  tnm <- peekCText (topic'RdKafkaMetadataTopicT tm)
  pts <- peekArray (partitionCnt'RdKafkaMetadataTopicT tm) (partitions'RdKafkaMetadataTopicT tm)
  pms <- mapM fromPartitionMetadataPtr pts
  return
    TopicMetadata
      { tmTopicName = TopicName tnm,
        tmPartitions = pms,
        tmError = kafkaErrorToMaybe $ KafkaResponseError (err'RdKafkaMetadataTopicT tm)
      }

fromPartitionMetadataPtr :: RdKafkaMetadataPartitionT -> IO PartitionMetadata
fromPartitionMetadataPtr pm = do
  reps <- peekArray (replicaCnt'RdKafkaMetadataPartitionT pm) (replicas'RdKafkaMetadataPartitionT pm)
  isrs <- peekArray (isrCnt'RdKafkaMetadataPartitionT pm) (isrs'RdKafkaMetadataPartitionT pm)
  return
    PartitionMetadata
      { pmPartitionId = PartitionId (id'RdKafkaMetadataPartitionT pm),
        pmError = kafkaErrorToMaybe $ KafkaResponseError (err'RdKafkaMetadataPartitionT pm),
        pmLeader = BrokerId (leader'RdKafkaMetadataPartitionT pm),
        pmReplicas = BrokerId . fromIntegral <$> reps,
        pmInSyncReplicas = BrokerId . fromIntegral <$> isrs
      }

fromBrokerMetadataPtr :: RdKafkaMetadataBrokerT -> IO BrokerMetadata
fromBrokerMetadataPtr bm = do
  host <- peekCText (host'RdKafkaMetadataBrokerT bm)
  return
    BrokerMetadata
      { bmBrokerId = BrokerId (id'RdKafkaMetadataBrokerT bm),
        bmBrokerHost = host,
        bmBrokerPort = port'RdKafkaMetadataBrokerT bm
      }

fromKafkaMetadataPtr :: RdKafkaMetadataTPtr -> IO KafkaMetadata
fromKafkaMetadataPtr ptr =
  withForeignPtr ptr $ \realPtr -> do
    km <- peek realPtr
    pbms <- peekArray (brokerCnt'RdKafkaMetadataT km) (brokers'RdKafkaMetadataT km)
    bms <- mapM fromBrokerMetadataPtr pbms
    ptms <- peekArray (topicCnt'RdKafkaMetadataT km) (topics'RdKafkaMetadataT km)
    tms <- mapM fromTopicMetadataPtr ptms
    return
      KafkaMetadata
        { kmBrokers = bms,
          kmTopics = tms,
          kmOrigBroker = BrokerId $ fromIntegral (origBrokerId'RdKafkaMetadataT km)
        }

groupStateFromKafkaString :: Text -> Either KafkaError GroupState
groupStateFromKafkaString s = case s of
  "PreparingRebalance" -> Right GroupPreparingRebalance
  "CompletingRebalance" -> Right GroupAwaitingSync
  "AwaitingSync" -> Right GroupAwaitingSync
  "Stable" -> Right GroupStable
  "Dead" -> Right GroupDead
  "Empty" -> Right GroupEmpty
  _ -> Left (KafkaError ("Unsupported Kafka consumer group state: " <> s))
