module Kafka.Topic.Types
  ( PartitionCount (..),
    ReplicationFactor (..),
    NewTopic (..),
  )
where

import Data.Map

import Kafka.Types

newtype PartitionCount = PartitionCount {unPartitionCount :: Int}
  deriving stock (Show, Eq)

newtype ReplicationFactor = ReplicationFactor {unReplicationFactor :: Int}
  deriving stock (Show, Eq)

data NewTopic = NewTopic
  { topicName :: TopicName,
    topicPartitionCount :: PartitionCount,
    topicReplicationFactor :: ReplicationFactor,
    topicConfig :: Map String String
  }
  deriving stock (Show)
