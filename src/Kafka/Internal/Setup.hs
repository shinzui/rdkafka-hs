module Kafka.Internal.Setup
  ( KafkaProps (..),
    TopicProps (..),
    Kafka (..),
    KafkaConf (..),
    PendingMessages (..),
    emptyPendingMessages,
    pendingMessagePartition,
    resetPendingMessages,
    discardPendingMessages,
    forgetPendingPartitions,
    PendingMessage (..),
    PendingTimestamp (..),
    TopicConf (..),
    HasKafka (..),
    HasKafkaConf (..),
    HasTopicConf (..),
    Callback (..),
    CallbackPollStatus (..),
    ClientLifecycle (..),
    getRdKafka,
    getRdKafkaConf,
    getRdMsgQueue,
    getRdTopicConf,
    newTopicConf,
    newKafkaConf,
    kafkaConf,
    topicConf,
    checkConfSetValue,
    setKafkaConfValue,
    setAllKafkaConfValues,
    setTopicConfValue,
    setAllTopicConfValues,
  )
where

import Kafka.Internal.CallbackOwner (CallbackOwner, newCallbackOwner)
import Kafka.Internal.RdKafka (CCharBufPointer, RdKafkaConfResT (..), RdKafkaConfTPtr, RdKafkaQueueTPtr, RdKafkaTPtr, RdKafkaTopicConfTPtr, nErrorBytes, newRdKafkaConfT, newRdKafkaTopicConfT, rdKafkaConfSet, rdKafkaTopicConfSet)
import Kafka.Types (Headers, KafkaError (..), Millis, PartitionId, TopicName)

import Control.Concurrent.MVar (MVar, newMVar)
import Control.Exception (throw, throwIO)
import Data.ByteString (ByteString)
import Data.IORef (IORef, atomicModifyIORef', newIORef, readIORef)
import Data.Int (Int64)
import Data.Map (Map)
import Data.Sequence (Seq)
import Data.Sequence qualified as Seq
import Data.Set (Set)
import Data.Set qualified as Set
import Data.Text (Text)
import Foreign.C.String (peekCString)
import Foreign.Marshal.Alloc (allocaBytes)

import Data.Map qualified as Map
import Data.Text qualified as Text

--
-- Configuration
--
newtype KafkaProps = KafkaProps (Map Text Text)
  deriving stock (Show, Eq)

newtype TopicProps = TopicProps (Map Text Text)
  deriving stock (Show, Eq)

newtype Kafka = Kafka RdKafkaTPtr
  deriving stock (Show)

newtype TopicConf = TopicConf RdKafkaTopicConfTPtr
  deriving stock (Show)

-- | Callbacks allow retrieving various information like error occurences, statistics
-- and log messages.
-- See `Kafka.Consumer.setCallback` (Consumer) and `Kafka.Producer.setCallback` (Producer) for more details.
newtype Callback = Callback (KafkaConf -> IO ())

data CallbackPollStatus = CallbackPollEnabled | CallbackPollDisabled
  deriving stock (Show, Eq)

-- | Where a consumer is in its teardown. 'Kafka.Consumer.closeConsumer' moves
-- an open consumer to 'ClientClosed'; 'Kafka.Consumer.destroyConsumer' moves
-- any consumer to 'ClientDestroyed'. Both hold the 'MVar' for the whole
-- transition, so a concurrent caller waits for it to finish.
data ClientLifecycle = ClientOpen | ClientClosed | ClientDestroyed
  deriving stock (Show, Eq)

data KafkaConf = KafkaConf
  { -- | A pointer to a native Kafka configuration
    kcfgKafkaConfPtr :: RdKafkaConfTPtr,
    -- | A queue for messages
    kcfgMessagesQueue :: IORef (Maybe RdKafkaQueueTPtr),
    -- | A mutex to prevent handling callbacks from multiple threads
    -- which can be dangerous in some cases.
    kcfgCallbackPollStatus :: MVar CallbackPollStatus,
    -- | Serializes consumer close and destroy.
    kcfgLifecycle :: MVar ClientLifecycle,
    -- | Persistent callback bridges, transferred to the native client finalizer.
    kcfgCallbackOwner :: CallbackOwner,
    -- | Copied records/errors returned while serving callbacks. No native owners.
    kcfgPendingMessages :: IORef PendingMessages
  }

-- Kept independent of Consumer.Types, which uses KafkaConf for its client.
data PendingTimestamp = PendingNoTimestamp | PendingCreateTime Millis | PendingLogAppendTime Millis

data PendingMessage
  = PendingMessageError KafkaError (Maybe TopicName) (Maybe PartitionId) (Maybe Int64)
  | PendingMessageRecord TopicName PartitionId Int64 PendingTimestamp Headers (Maybe ByteString) (Maybe ByteString)

data PendingMessages = PendingMessages
  { bufferedMessages :: Seq PendingMessage,
    pendingPausedPartitions :: Set (TopicName, PartitionId)
  }

emptyPendingMessages :: PendingMessages
emptyPendingMessages = PendingMessages Seq.empty Set.empty

pendingMessagePartition :: PendingMessage -> Maybe (TopicName, PartitionId)
pendingMessagePartition (PendingMessageError _ topic partition _) = (,) <$> topic <*> partition
pendingMessagePartition (PendingMessageRecord topic partition _ _ _ _ _) = Just (topic, partition)

resetPendingMessages :: KafkaConf -> IO ()
resetPendingMessages conf = atomicModifyIORef' (kcfgPendingMessages conf) (const (emptyPendingMessages, ()))

-- Successful seeks replace cached records only for affected partitions.
discardPendingMessages :: KafkaConf -> Set (TopicName, PartitionId) -> IO ()
discardPendingMessages conf partitions = atomicModifyIORef' (kcfgPendingMessages conf) $ \buffer ->
  (buffer {bufferedMessages = Seq.filter (maybe True (`Set.notMember` partitions) . pendingMessagePartition) (bufferedMessages buffer)}, ())

-- Revocation also forgets the application's pause state for those partitions.
forgetPendingPartitions :: KafkaConf -> Set (TopicName, PartitionId) -> IO ()
forgetPendingPartitions conf partitions = atomicModifyIORef' (kcfgPendingMessages conf) $ \buffer ->
  (buffer {bufferedMessages = Seq.filter (maybe True (`Set.notMember` partitions) . pendingMessagePartition) (bufferedMessages buffer), pendingPausedPartitions = pendingPausedPartitions buffer `Set.difference` partitions}, ())

class HasKafka a where
  getKafka :: a -> Kafka

class HasKafkaConf a where
  getKafkaConf :: a -> KafkaConf

class HasTopicConf a where
  getTopicConf :: a -> TopicConf

instance HasKafkaConf KafkaConf where
  getKafkaConf = id
  {-# INLINE getKafkaConf #-}

instance HasKafka Kafka where
  getKafka = id
  {-# INLINE getKafka #-}

instance HasTopicConf TopicConf where
  getTopicConf = id
  {-# INLINE getTopicConf #-}

getRdKafka :: (HasKafka k) => k -> RdKafkaTPtr
getRdKafka k = let (Kafka k') = getKafka k in k'
{-# INLINE getRdKafka #-}

getRdKafkaConf :: (HasKafkaConf k) => k -> RdKafkaConfTPtr
getRdKafkaConf k = let (KafkaConf k' _ _ _ _ _) = getKafkaConf k in k'
{-# INLINE getRdKafkaConf #-}

getRdMsgQueue :: (HasKafkaConf k) => k -> IO (Maybe RdKafkaQueueTPtr)
getRdMsgQueue k =
  let (KafkaConf _ rq _ _ _ _) = getKafkaConf k
   in readIORef rq

getRdTopicConf :: (HasTopicConf t) => t -> RdKafkaTopicConfTPtr
getRdTopicConf t = let (TopicConf t') = getTopicConf t in t'
{-# INLINE getRdTopicConf #-}

newTopicConf :: IO TopicConf
newTopicConf = TopicConf <$> newRdKafkaTopicConfT

newKafkaConf :: IO KafkaConf
newKafkaConf = KafkaConf <$> newRdKafkaConfT <*> newIORef Nothing <*> newMVar CallbackPollEnabled <*> newMVar ClientOpen <*> newCallbackOwner <*> newIORef emptyPendingMessages

kafkaConf :: KafkaProps -> IO KafkaConf
kafkaConf overrides = do
  conf <- newKafkaConf
  setAllKafkaConfValues conf overrides
  return conf

topicConf :: TopicProps -> IO TopicConf
topicConf overrides = do
  conf <- newTopicConf
  setAllTopicConfValues conf overrides
  return conf

checkConfSetValue :: RdKafkaConfResT -> CCharBufPointer -> IO ()
checkConfSetValue err charPtr = case err of
  RdKafkaConfOk -> return ()
  RdKafkaConfInvalid -> do
    str <- peekCString charPtr
    throw $ KafkaInvalidConfigurationValue (Text.pack str)
  RdKafkaConfUnknown -> do
    str <- peekCString charPtr
    throw $ KafkaUnknownConfigurationKey (Text.pack str)

setKafkaConfValue :: KafkaConf -> Text -> Text -> IO ()
setKafkaConfValue (KafkaConf confPtr _ _ _ _ _) key value = do
  validateConfigStrings key value
  allocaBytes nErrorBytes $ \charPtr -> do
    err <- rdKafkaConfSet confPtr (Text.unpack key) (Text.unpack value) charPtr (fromIntegral nErrorBytes)
    checkConfSetValue err charPtr

setAllKafkaConfValues :: KafkaConf -> KafkaProps -> IO ()
setAllKafkaConfValues conf (KafkaProps props) = Map.foldMapWithKey (setKafkaConfValue conf) props -- forM_ props $ uncurry (setKafkaConfValue conf)

setTopicConfValue :: TopicConf -> Text -> Text -> IO ()
setTopicConfValue (TopicConf confPtr) key value = do
  validateConfigStrings key value
  allocaBytes nErrorBytes $ \charPtr -> do
    err <- rdKafkaTopicConfSet confPtr (Text.unpack key) (Text.unpack value) charPtr (fromIntegral nErrorBytes)
    checkConfSetValue err charPtr

setAllTopicConfValues :: TopicConf -> TopicProps -> IO ()
setAllTopicConfValues conf (TopicProps props) = Map.foldMapWithKey (setTopicConfValue conf) props -- forM_ props $ uncurry (setTopicConfValue conf)

-- Native setters borrow NUL-terminated strings; accepting embedded NULs would
-- validate and apply only a prefix of the supplied property.
validateConfigStrings :: Text -> Text -> IO ()
validateConfigStrings key value
  | Text.any (== '\0') key = throwIO (KafkaInvalidConfigurationValue "Configuration key contains NUL")
  | Text.any (== '\0') value = throwIO (KafkaInvalidConfigurationValue "Configuration value contains NUL")
  | otherwise = pure ()
