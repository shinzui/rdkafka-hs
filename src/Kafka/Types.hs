{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings #-}

-----------------------------------------------------------------------------

-----------------------------------------------------------------------------

-- |
-- Module holding types shared by consumer and producer modules.
module Kafka.Types
  ( BrokerId (..),
    PartitionId (..),
    Millis (..),
    ClientId (..),
    BatchSize (..),
    TopicName (..),
    BrokerAddress (..),
    Timeout (..),
    KafkaLogLevel (..),
    KafkaError (..),
    KafkaDebug (..),
    KafkaCompressionCodec (..),
    TopicType (..),
    Headers,
    headersFromList,
    headersToList,
    topicType,
    kafkaDebugToText,
    kafkaCompressionCodecToText,
  )
where

import Control.Exception (Exception (..))
import Data.ByteString qualified as BS
import Data.Int (Int64)
import Data.String (IsString)
import Data.Text (Text, isPrefixOf)
import GHC.Generics (Generic)
import Kafka.Internal.RdKafka (RdKafkaRespErrT, rdKafkaErr2name, rdKafkaErr2str)

-- | Kafka broker ID
newtype BrokerId = BrokerId {unBrokerId :: Int}
  deriving stock (Show, Eq, Ord, Read, Generic)

-- | Topic partition ID
newtype PartitionId = PartitionId {unPartitionId :: Int}
  deriving stock (Show, Eq, Read, Ord, Generic)
  deriving newtype (Enum)

-- | A number of milliseconds, used to represent durations and timestamps
newtype Millis = Millis {unMillis :: Int64}
  deriving stock (Show, Read, Eq, Ord, Generic)
  deriving newtype (Num)

-- | Client ID used by Kafka to better track requests
--
-- See <https://kafka.apache.org/documentation/#client.id Kafka documentation on client ID>
newtype ClientId = ClientId
  { unClientId :: Text
  }
  deriving stock (Show, Eq, Ord, Generic)
  deriving newtype (IsString)

-- | Batch size used for polling
newtype BatchSize = BatchSize {unBatchSize :: Int}
  deriving stock (Show, Read, Eq, Ord, Generic)
  deriving newtype (Num)

-- | Whether the topic is created by a user or by the system
data TopicType
  = -- | Normal topics that are created by user.
    User
  | -- | Topics starting with a double underscore "\__" (@__consumer_offsets@, @__confluent.support.metrics@, etc.) are considered "system" topics
    System
  deriving stock (Show, Read, Eq, Ord, Generic)

-- | Topic name to consume/produce messages
--
-- Wildcard (regex) topics are supported by the /librdkafka/ assignor:
-- any topic name in the topics list that is prefixed with @^@ will
-- be regex-matched to the full list of topics in the cluster and matching
-- topics will be added to the subscription list.
newtype TopicName = TopicName
  { -- | a simple topic name or a regex if started with @^@
    unTopicName :: Text
  }
  deriving stock (Show, Eq, Ord, Read, Generic)
  deriving newtype (IsString)

-- | Deduce the type of a topic from its name, by checking if it starts with a double underscore "\__"
topicType :: TopicName -> TopicType
topicType (TopicName tn) =
  if "__" `isPrefixOf` tn then System else User
{-# INLINE topicType #-}

-- | Kafka broker address string (e.g. @broker1:9092@)
newtype BrokerAddress = BrokerAddress
  { unBrokerAddress :: Text
  }
  deriving stock (Show, Eq, Generic)
  deriving newtype (IsString)

-- | Timeout in milliseconds
newtype Timeout = Timeout {unTimeout :: Int}
  deriving stock (Show, Eq, Read, Generic)

-- | Log levels for /librdkafka/.
data KafkaLogLevel
  = KafkaLogEmerg
  | KafkaLogAlert
  | KafkaLogCrit
  | KafkaLogErr
  | KafkaLogWarning
  | KafkaLogNotice
  | KafkaLogInfo
  | KafkaLogDebug
  deriving stock (Show, Enum, Eq)

-- | All possible Kafka errors
data KafkaError
  = KafkaError Text
  | KafkaInvalidReturnValue
  | KafkaBadSpecification Text
  | KafkaResponseError RdKafkaRespErrT
  | KafkaInvalidConfigurationValue Text
  | KafkaUnknownConfigurationKey Text
  | KafkaBadConfiguration
  deriving stock (Eq, Show, Generic)

instance Exception KafkaError where
  displayException (KafkaResponseError err) =
    "[" ++ rdKafkaErr2name err ++ "] " ++ rdKafkaErr2str err
  displayException err = show err

-- | Available /librdkafka/ debug contexts
data KafkaDebug
  = DebugGeneric
  | DebugBroker
  | DebugTopic
  | DebugMetadata
  | DebugQueue
  | DebugMsg
  | DebugProtocol
  | DebugCgrp
  | DebugSecurity
  | DebugFetch
  | DebugFeature
  | DebugAll
  deriving stock (Eq, Show, Generic)

-- | Convert a 'KafkaDebug' into its /librdkafka/ string equivalent.
--
-- This is used internally by the library but may be useful to some developers.
kafkaDebugToText :: KafkaDebug -> Text
kafkaDebugToText d = case d of
  DebugGeneric -> "generic"
  DebugBroker -> "broker"
  DebugTopic -> "topic"
  DebugMetadata -> "metadata"
  DebugQueue -> "queue"
  DebugMsg -> "msg"
  DebugProtocol -> "protocol"
  DebugCgrp -> "cgrp"
  DebugSecurity -> "security"
  DebugFetch -> "fetch"
  DebugFeature -> "feature"
  DebugAll -> "all"

-- | Compression codec used by a topic
--
-- See <https://kafka.apache.org/documentation/#compression.type Kafka documentation on compression codecs>
data KafkaCompressionCodec
  = NoCompression
  | Gzip
  | Snappy
  | Lz4
  | Zstd
  deriving stock (Eq, Show, Generic)

-- | Convert a 'KafkaCompressionCodec' into its /librdkafka/ string equivalent.
--
-- This is used internally by the library but may be useful to some developers.
kafkaCompressionCodecToText :: KafkaCompressionCodec -> Text
kafkaCompressionCodecToText c = case c of
  NoCompression -> "none"
  Gzip -> "gzip"
  Snappy -> "snappy"
  Lz4 -> "lz4"
  Zstd -> "zstd"

-- | Headers that might be passed along with a record: an ordered list of
-- @(name, value)@ pairs. Kafka allows a name to repeat, so the list keeps
-- every pair, duplicates included, in wire order. A producer sends the pairs
-- in list order and a consumer decodes them in the order they were sent;
-- selecting a value for a repeated name (for example the last occurrence) is
-- left to the caller.
newtype Headers = Headers {unHeaders :: [(BS.ByteString, BS.ByteString)]}
  deriving stock (Eq, Show, Read, Generic)
  deriving newtype (Semigroup, Monoid)

-- | Headers from @(name, value)@ pairs, sent in list order. Repeated names and
-- identical pairs are kept.
headersFromList :: [(BS.ByteString, BS.ByteString)] -> Headers
headersFromList = Headers

-- | The @(name, value)@ pairs in wire order, with every repeated name and
-- identical pair the record carried.
headersToList :: Headers -> [(BS.ByteString, BS.ByteString)]
headersToList = unHeaders
