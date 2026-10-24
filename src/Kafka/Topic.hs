module Kafka.Topic
  ( module X,
    createTopic,
    deleteTopic,
  )
where

import Control.Exception
import Control.Monad (join)
import Control.Monad.IO.Class
import Control.Monad.Trans.Except
import Data.Bifunctor
import Data.Foldable
import Data.List.NonEmpty
import Data.List.NonEmpty qualified as NEL
import Data.Map qualified as M
import Data.Text qualified as T
import Foreign.C.Types (CInt)
import Foreign.ForeignPtr (finalizeForeignPtr, withForeignPtr)
import GHC.Clock (getMonotonicTimeNSec)
import Kafka.Internal.RdKafka
import Kafka.Internal.Setup

import Kafka.Topic.Types as X
import Kafka.Types as X

-- | Create a topic with the supplied broker-side settings. Invalid C-string
-- inputs or unrepresentable counts return 'KafkaBadSpecification'; other
-- validation is performed by librdkafka and the broker; unknown-property
-- behavior follows the broker. The request wait follows
-- @socket.timeout.ms@ with up to one extra second for native error delivery.
-- A timeout leaves the broker's eventual outcome unknown.

--- CREATE TOPIC ---
createTopic :: (HasKafka k) => k -> NewTopic -> IO (Either KafkaError TopicName)
createTopic k topic
  | Just err <- validateNewTopic topic = pure (Left err)
  | otherwise = withAdminResources (getRdKafka k) $ \queue opts requestTimeout -> do
      let kafkaPtr = getRdKafka k
      topicRes <- withNewTopic topic $ \topic' -> rdKafkaCreateTopic kafkaPtr topic' opts queue

      case topicRes of
        Left err -> do
          pure $ Left (NEL.head err)
        Right _ -> do
          waitForResponse (topicName topic) rdKafkaEventCreateTopicsResult rdKafkaCreateTopicsResultTopics queue requestTimeout

-- | Delete a topic. The request wait follows @socket.timeout.ms@, with up
-- to one second for native error delivery. A timeout leaves the broker's
-- eventual outcome unknown. Embedded NUL names are rejected before
-- calling the native API.

--- DELETE TOPIC ---
deleteTopic ::
  (HasKafka k) =>
  k ->
  TopicName ->
  IO (Either KafkaError TopicName)
deleteTopic k topic
  | Just err <- validateTopicName topic = pure (Left err)
  | otherwise = liftIO $ withAdminResources (getRdKafka k) $ \queue opts requestTimeout -> do
      let kafkaPtr = getRdKafka k
      topicRes <- withOldTopic topic $ \topic' -> rdKafkaDeleteTopics kafkaPtr [topic'] opts queue
      case topicRes of
        Left err -> do
          pure $ Left (NEL.head err)
        Right _ -> do
          waitForResponse topic rdKafkaEventDeleteTopicsResult rdKafkaDeleteTopicsResultTopics queue requestTimeout

-- C strings cannot represent NUL and C integer conversion must not wrap a
-- count into another valid request. Other values retain native validation.
validateTopicName :: TopicName -> Maybe KafkaError
validateTopicName (TopicName name)
  | T.any (== '\0') name = Just (KafkaBadSpecification "topic name must not contain NUL")
  | otherwise = Nothing

validateNewTopic :: NewTopic -> Maybe KafkaError
validateNewTopic topic
  | Just err <- validateTopicName (topicName topic) = Just err
  | outsideCInt (unPartitionCount (topicPartitionCount topic)) =
      Just (KafkaBadSpecification "partition count is outside the C int range")
  | outsideCInt (unReplicationFactor (topicReplicationFactor topic)) =
      Just (KafkaBadSpecification "replication factor is outside the C int range")
  | any (\(key, value) -> '\0' `elem` key || '\0' `elem` value) (M.toList (topicConfig topic)) =
      Just (KafkaBadSpecification "topic configuration keys and values must not contain NUL")
  | otherwise = Nothing
  where
    outsideCInt value = value < fromIntegral (minBound :: CInt) || value > fromIntegral (maxBound :: CInt)

withNewTopic ::
  NewTopic ->
  (RdKafkaNewTopicTPtr -> IO a) ->
  IO (Either (NonEmpty KafkaError) a)
withNewTopic t action =
  fmap join $ withUnsafeOne t mkNewTopicUnsafe rdKafkaNewTopicDestroy $ \native -> do
    configured <- whileRight (uncurry $ rdKafkaNewTopicSetConfig native) (M.toList $ topicConfig t)
    case configured of
      Left err -> pure (Left (singletonList (KafkaResponseError err)))
      Right () -> Right <$> action native

withOldTopic ::
  TopicName ->
  (RdKafkaDeleteTopicTPtr -> IO a) ->
  IO (Either (NonEmpty KafkaError) a)
withOldTopic tName transform =
  bracket (oldTopicPtr tName) (mapM_ finalizeForeignPtr) $ \case
    Left err -> pure (Left (singletonList err))
    Right native -> Right <$> transform native

oldTopicPtr :: TopicName -> IO (Either KafkaError RdKafkaDeleteTopicTPtr)
oldTopicPtr tName = do
  res <- newRdKafkaDeleteTopic $ T.unpack . unTopicName $ tName
  case res of
    Left str -> pure $ Left (KafkaError $ T.pack str)
    Right ptr -> pure $ Right ptr

mkNewTopicUnsafe :: NewTopic -> IO (Either KafkaError RdKafkaNewTopicTPtr)
mkNewTopicUnsafe topic = do
  allocated <- newRdKafkaNewTopicUnsafe (T.unpack $ unTopicName $ topicName topic) (unPartitionCount $ topicPartitionCount topic) (unReplicationFactor $ topicReplicationFactor topic)
  pure (first (KafkaError . T.pack) allocated)

-- Keep the client alive through request completion. Each ForeignPtr owns one
-- finalizer; explicit finalization consumes it, so GC cannot free it twice.
withAdminResources :: RdKafkaTPtr -> (RdKafkaQueueTPtr -> RdKafkaAdminOptionsTPtr -> Int -> IO (Either KafkaError a)) -> IO (Either KafkaError a)
withAdminResources kafka action = withForeignPtr kafka $ \_ ->
  rdKafkaSocketTimeout kafka >>= \case
    Left err -> pure (Left (KafkaError (T.pack err)))
    Right requestTimeout ->
      bracket (newRdKafkaQueue kafka) finalizeForeignPtr $ \queue ->
        bracket (newRdKAdminOptions kafka RdKafkaAdminOpAny) finalizeForeignPtr $ \options ->
          action queue options requestTimeout

withUnsafeOne ::
  -- | Item to handle
  a ->
  -- | Create an unsafe element
  (a -> IO (Either KafkaError b)) ->
  -- | Destroy the unsafe element
  (b -> IO ()) ->
  -- | Handler
  (b -> IO c) ->
  IO (Either (NonEmpty KafkaError) c)
withUnsafeOne a mkOne cleanup f =
  bracket (mkOne a) cleanupOne processOne
  where
    cleanupOne (Right b) = cleanup b
    cleanupOne (Left _) = pure () -- no resource to clean if creation failed
    processOne (Right b) = Right <$> f b
    processOne (Left e) = pure (Left (singletonList e))

whileRight ::
  (Monad m) =>
  (a -> m (Either e ())) ->
  [a] ->
  m (Either e ())
whileRight f as = runExceptT $ traverse_ (ExceptT . f) as

-- One public request contains one topic. Decode the event while it is owned;
-- require that exact topic, and preserve request-level and per-topic errors.
waitForResponse ::
  TopicName ->
  (RdKafkaEventTPtr -> IO (Maybe a)) ->
  (a -> IO [Either (String, RdKafkaRespErrT, String) String]) ->
  RdKafkaQueueTPtr ->
  Int ->
  IO (Either KafkaError TopicName)
waitForResponse topic fromEvent toResults queue requestTimeout = do
  -- Native requests use socket.timeout.ms. Keep one poll quantum of grace
  -- for the native worker to deliver its terminal error at that deadline.
  deadline <- (+ ((fromIntegral requestTimeout + 1000) * 1000000)) <$> getMonotonicTimeNSec
  let go = do
        now <- getMonotonicTimeNSec
        if now >= deadline
          then pure (Left (KafkaResponseError RdKafkaRespErrTimedOut))
          else do
            let remaining = fromIntegral (min 1000 (max 1 ((deadline - now) `div` 1000000)))
            result <- bracket (rdKafkaQueuePoll queue remaining) (mapM_ finalizeForeignPtr) $ \case
              Nothing -> pure Nothing
              Just event -> do
                requestError <- rdKafkaEventError event
                if requestError /= RdKafkaRespErrNoError
                  then pure (Just (Left (KafkaResponseError requestError)))
                  else
                    fromEvent event >>= \case
                      Nothing -> pure (Just (Left KafkaInvalidReturnValue))
                      Just response -> do
                        results <- toResults response
                        pure $ Just $ case results of
                          [Right name] | TopicName (T.pack name) == topic -> Right topic
                          [Left (name, err, _)] | TopicName (T.pack name) == topic -> Left (KafkaResponseError err)
                          _ -> Left KafkaInvalidReturnValue
            maybe go pure result
  go

singletonList :: a -> NonEmpty a
singletonList x = x :| []
