{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

-----------------------------------------------------------------------------

-----------------------------------------------------------------------------

-- |
-- Module holding consumer types.
module Kafka.Consumer.Types
  ( KafkaConsumer (..),
    ConsumerGroupId (..),
    Offset (..),
    OffsetReset (..),
    RebalanceEvent (..),
    PartitionOffset (..),
    SubscribedPartitions (..),
    Timestamp (..),
    OffsetCommit (..),
    OffsetStoreSync (..),
    OffsetStoreMethod (..),
    TopicPartition (..),
    ConsumerRecord (..),
    ConsumerMessageError (..),
    crMapKey,
    crMapValue,
    crMapKV,
    -- why are these here?

    -- * Deprecated
    sequenceFirst,
    traverseFirst,
    traverseFirstM,
    traverseM,
    bitraverseM,
  )
where

import Data.Bifoldable (Bifoldable (..))
import Data.Bifunctor (Bifunctor (..))
import Data.Bitraversable (Bitraversable (..), bimapM, bisequence)
import Data.Int (Int64)
import Data.String (IsString)
import Data.Text (Text)
import GHC.Generics (Generic)
import Kafka.Internal.Setup (HasKafka (..), HasKafkaConf (..), Kafka (..), KafkaConf (..))
import Kafka.Types (Headers, KafkaError, Millis (..), PartitionId (..), TopicName (..))

-- | The main type for Kafka consumption, used e.g. to poll and commit messages.
--
-- Its constructor is intentionally not exposed, instead, one should use 'Kafka.Consumer.newConsumer' to acquire such a value.
data KafkaConsumer = KafkaConsumer
  { kcKafkaPtr :: !Kafka,
    kcKafkaConf :: !KafkaConf
  }

instance HasKafka KafkaConsumer where
  getKafka = kcKafkaPtr
  {-# INLINE getKafka #-}

instance HasKafkaConf KafkaConsumer where
  getKafkaConf = kcKafkaConf
  {-# INLINE getKafkaConf #-}

-- | Consumer group ID. Different consumers with the same consumer group ID will get assigned different partitions of each subscribed topic.
--
-- See <https://kafka.apache.org/documentation/#group.id Kafka documentation on consumer group>
newtype ConsumerGroupId = ConsumerGroupId
  { unConsumerGroupId :: Text
  }
  deriving stock (Show, Ord, Eq, Generic)
  deriving newtype (IsString)

-- | A message offset in a partition
newtype Offset = Offset {unOffset :: Int64}
  deriving stock (Show, Eq, Ord, Read, Generic)

-- | Where to reset the offset when there is no initial offset in Kafka
--
-- See <https://kafka.apache.org/documentation/#auto.offset.reset Kafka documentation on offset reset>
data OffsetReset = Earliest | Latest
  deriving stock (Show, Eq, Generic)

-- | A set of events which happen during the rebalancing process
data RebalanceEvent
  = -- | Happens before Kafka Client confirms new assignment
    RebalanceBeforeAssign [(TopicName, PartitionId)]
  | -- | Happens after the new assignment is confirmed
    RebalanceAssign [(TopicName, PartitionId)]
  | -- | Happens before Kafka Client confirms partitions rejection
    RebalanceBeforeRevoke [(TopicName, PartitionId)]
  | -- | Happens after the rejection is confirmed
    RebalanceRevoke [(TopicName, PartitionId)]
  deriving stock (Eq, Show, Generic)

-- | The partition offset
data PartitionOffset
  = PartitionOffsetBeginning
  | PartitionOffsetEnd
  | PartitionOffset Int64
  | PartitionOffsetStored
  | PartitionOffsetInvalid
  deriving stock (Eq, Show, Generic)

-- | Partitions subscribed by a consumer
data SubscribedPartitions
  = -- | Subscribe only to those partitions
    SubscribedPartitions [PartitionId]
  | -- | Subscribe to all partitions
    SubscribedPartitionsAll
  deriving stock (Show, Eq, Generic)

-- | Consumer record timestamp
data Timestamp
  = CreateTime !Millis
  | LogAppendTime !Millis
  | NoTimestamp
  deriving stock (Show, Eq, Read, Generic)

-- | Offsets commit mode
data OffsetCommit
  = -- | Forces consumer to block until the broker offsets commit is done
    OffsetCommit
  | -- | Offsets will be committed in a non-blocking way
    OffsetCommitAsync
  deriving stock (Show, Eq, Generic)

-- | Indicates how offsets are to be synced to disk
data OffsetStoreSync
  = -- | Do not sync offsets (in Kafka: -1)
    OffsetSyncDisable
  | -- | Sync immediately after each offset commit (in Kafka: 0)
    OffsetSyncImmediate
  | -- | Sync after specified interval in millis
    OffsetSyncInterval Int
  deriving stock (Show, Eq, Generic)

-- | Indicates the method of storing the offsets
data OffsetStoreMethod
  = -- | Offsets are stored in Kafka broker (preferred)
    OffsetStoreBroker
  | -- | Offsets are stored in a file (and synced to disk according to the sync policy)
    OffsetStoreFile FilePath OffsetStoreSync
  deriving stock (Show, Eq, Generic)

-- | Kafka topic partition structure
data TopicPartition = TopicPartition
  { tpTopicName :: TopicName,
    tpPartition :: PartitionId,
    tpOffset :: PartitionOffset
  }
  deriving stock (Show, Eq, Generic)

-- | Represents a /received/ message from Kafka (i.e. used in a consumer)
-- | A per-message error from 'Kafka.Consumer.pollMessageAttributed', with the
-- topic, partition and offset librdkafka attached to the errored message.
--
-- For @'Kafka.Types.KafkaResponseError' 'Kafka.Consumer.RdKafkaRespErrPartitionEof'@
-- (delivered when @enable.partition.eof@ is @true@) the offset is the
-- partition's end offset. Each origin field is 'Nothing' when librdkafka did
-- not set it, and all three are 'Nothing' for errors that belong to no message:
-- a poll timeout or a raised fatal error.
data ConsumerMessageError = ConsumerMessageError
  { cmeError :: !KafkaError,
    cmeTopic :: !(Maybe TopicName),
    cmePartition :: !(Maybe PartitionId),
    cmeOffset :: !(Maybe Offset)
  }
  deriving stock (Eq, Show, Generic)

data ConsumerRecord k v = ConsumerRecord
  { -- | Kafka topic this message was received from
    crTopic :: !TopicName,
    -- | Kafka partition this message was received from
    crPartition :: !PartitionId,
    -- | Offset within the 'crPartition' Kafka partition
    crOffset :: !Offset,
    -- | Message timestamp
    crTimestamp :: !Timestamp,
    -- | Message headers
    crHeaders :: !Headers,
    -- | Message key
    crKey :: !k,
    -- | Message value
    crValue :: !v
  }
  deriving stock (Eq, Show, Read, Generic)

instance Bifunctor ConsumerRecord where
  bimap f g (ConsumerRecord t p o ts hds k v) = ConsumerRecord t p o ts hds (f k) (g v)
  {-# INLINE bimap #-}

instance Functor (ConsumerRecord k) where
  fmap = second
  {-# INLINE fmap #-}

instance Foldable (ConsumerRecord k) where
  foldMap f r = f (crValue r)
  {-# INLINE foldMap #-}

instance Traversable (ConsumerRecord k) where
  traverse f r = (\v -> crMapValue (const v) r) <$> f (crValue r)
  {-# INLINE traverse #-}

instance Bifoldable ConsumerRecord where
  bifoldMap f g r = f (crKey r) `mappend` g (crValue r)
  {-# INLINE bifoldMap #-}

instance Bitraversable ConsumerRecord where
  bitraverse f g r = (\k v -> bimap (const k) (const v) r) <$> f (crKey r) <*> g (crValue r)
  {-# INLINE bitraverse #-}

{-# DEPRECATED crMapKey "Isn't concern of this library. Use 'first'" #-}
crMapKey :: (k -> k') -> ConsumerRecord k v -> ConsumerRecord k' v
crMapKey = first
{-# INLINE crMapKey #-}

{-# DEPRECATED crMapValue "Isn't concern of this library. Use 'second'" #-}
crMapValue :: (v -> v') -> ConsumerRecord k v -> ConsumerRecord k v'
crMapValue = second
{-# INLINE crMapValue #-}

{-# DEPRECATED crMapKV "Isn't concern of this library. Use 'bimap'" #-}
crMapKV :: (k -> k') -> (v -> v') -> ConsumerRecord k v -> ConsumerRecord k' v'
crMapKV = bimap
{-# INLINE crMapKV #-}

{-# DEPRECATED sequenceFirst "Isn't concern of this library. Use @'bitraverse' 'id' 'pure'@" #-}
sequenceFirst :: (Bitraversable t, Applicative f) => t (f k) v -> f (t k v)
sequenceFirst = bitraverse id pure
{-# INLINE sequenceFirst #-}

{-# DEPRECATED traverseFirst "Isn't concern of this library. Use @'bitraverse' f 'pure'@" #-}
traverseFirst ::
  (Bitraversable t, Applicative f) =>
  (k -> f k') ->
  t k v ->
  f (t k' v)
traverseFirst f = bitraverse f pure
{-# INLINE traverseFirst #-}

{-# DEPRECATED traverseFirstM "Isn't concern of this library. Use @'bitraverse' 'id' 'pure' '<$>' 'bitraverse' f 'pure' r@" #-}
traverseFirstM ::
  (Bitraversable t, Applicative f, Monad m) =>
  (k -> m (f k')) ->
  t k v ->
  m (f (t k' v))
traverseFirstM f r = bitraverse id pure <$> bitraverse f pure r
{-# INLINE traverseFirstM #-}

{-# DEPRECATED traverseM "Isn't concern of this library. Use @'sequenceA' '<$>' 'traverse' f r@" #-}
traverseM ::
  (Traversable t, Applicative f, Monad m) =>
  (v -> m (f v')) ->
  t v ->
  m (f (t v'))
traverseM f r = sequenceA <$> traverse f r
{-# INLINE traverseM #-}

{-# DEPRECATED bitraverseM "Isn't concern of this library. Use @'Data.Bitraversable.bisequenceA' '<$>' 'bimapM' f g r@" #-}
bitraverseM ::
  (Bitraversable t, Applicative f, Monad m) =>
  (k -> m (f k')) ->
  (v -> m (f v')) ->
  t k v ->
  m (f (t k' v'))
bitraverseM f g r = bisequence <$> bimapM f g r
{-# INLINE bitraverseM #-}
