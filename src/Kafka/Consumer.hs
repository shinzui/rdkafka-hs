{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections #-}

-----------------------------------------------------------------------------

-----------------------------------------------------------------------------

-- |
-- Module to consume messages from Kafka topics.
--
-- Here's an example of code to consume messages from a topic:
--
-- @
-- import Control.Exception (bracket)
-- import Control.Monad (replicateM_)
-- import Kafka.Consumer
--
-- -- Global consumer properties
-- consumerProps :: 'ConsumerProperties'
-- consumerProps = 'brokersList' ["localhost:9092"]
--              <> 'groupId' ('ConsumerGroupId' "consumer_example_group")
--              <> 'noAutoCommit'
--              <> 'logLevel' 'KafkaLogInfo'
--
-- -- Subscription to topics
-- consumerSub :: 'Subscription'
-- consumerSub = 'topics' ['TopicName' "kafka-client-example-topic"]
--            <> 'offsetReset' 'Earliest'
--
-- -- Running an example
-- runConsumerExample :: IO ()
-- runConsumerExample = do
--     res <- bracket mkConsumer clConsumer runHandler
--     print res
--     where
--       mkConsumer = 'newConsumer' consumerProps consumerSub
--       clConsumer (Left err) = pure (Left err)
--       clConsumer (Right kc) = (maybe (Right ()) Left) \<$\> 'closeConsumer' kc
--       runHandler (Left err) = pure (Left err)
--       runHandler (Right kc) = processMessages kc
--
-- -- Example polling 10 times before stopping
-- processMessages :: 'KafkaConsumer' -> IO (Either 'KafkaError' ())
-- processMessages kafka = do
--     replicateM_ 10 $ do
--       msg <- 'pollMessage' kafka ('Timeout' 1000)
--       putStrLn $ "Message: " <> show msg
--       err <- 'commitAllOffsets' 'OffsetCommit' kafka
--       putStrLn $ "Offsets: " <> maybe "Committed." show err
--     pure $ Right ()
-- @
module Kafka.Consumer
  ( KafkaConsumer,
    module X,
    runConsumer,
    newConsumer,
    assign,
    assignment,
    subscription,
    pausePartitions,
    resumePartitions,
    committed,
    position,
    seek,
    seekPartitions,
    pollMessage,
    pollConsumerEvents,
    pollMessageBatch,
    consumerFatalError,
    assignmentLost,
    consumerClosed,
    setSaslCredentials,
    commitOffsetMessage,
    commitAllOffsets,
    commitPartitionsOffsets,
    storeOffsets,
    storeOffsetMessage,
    rewindConsumer,
    closeConsumer,
    -- ReExport Types
    RdKafkaRespErrT (..),
  )
where

import Control.Arrow (left, (&&&))
import Control.Concurrent (forkIO, modifyMVar, rtsSupportsBoundThreads, withMVar)
import Control.Exception (bracket)
import Control.Monad (forM_, unless, void, when)
import Control.Monad.IO.Class (MonadIO (liftIO))
import Control.Monad.Trans.Except (ExceptT (ExceptT), runExceptT)
import Data.Bifunctor (bimap, first)
import Data.ByteString qualified as BS
import Data.IORef (readIORef, writeIORef)
import Data.Map qualified as M hiding (foldr, map)
import Data.Maybe (fromMaybe)
import Data.Set (Set)
import Data.Set qualified as Set
import Data.Text (Text)
import Data.Text qualified as Text
import Foreign hiding (void)
import Kafka.Consumer.Convert (fromMessagePtr, fromNativeTopicPartitionList'', offsetCommitToBool, offsetToInt64, toMap, toNativeTopicPartitionList, toNativeTopicPartitionList', topicPartitionFromMessageForCommit)
import Kafka.Consumer.Types (KafkaConsumer (..))
import Kafka.Internal.RdKafka (RdKafkaRespErrT (..), RdKafkaTopicPartitionListTPtr, RdKafkaTypeT (..), newRdKafkaT, newRdKafkaTopicPartitionListT, newRdKafkaTopicT, rdKafkaAssign, rdKafkaAssignment, rdKafkaAssignmentLost, rdKafkaCommit, rdKafkaCommitted, rdKafkaConfSetDefaultTopicConf, rdKafkaConsumeBatchQueue, rdKafkaConsumeQueue, rdKafkaConsumerClose, rdKafkaConsumerClosed, rdKafkaConsumerPoll, rdKafkaErrorCode, rdKafkaErrorDestroy, rdKafkaFatalError, rdKafkaMessageDestroy, rdKafkaOffsetsStore, rdKafkaPausePartitions, rdKafkaPollSetConsumer, rdKafkaPosition, rdKafkaQueueDestroy, rdKafkaQueueNew, rdKafkaResumePartitions, rdKafkaSeek, rdKafkaSeekPartitions, rdKafkaSetLogLevel, rdKafkaSubscribe, rdKafkaSubscription, rdKafkaTopicConfDup, rdKafkaTopicPartitionListAdd)
import Kafka.Internal.Setup (Callback (..), CallbackPollStatus (..), Kafka (..), KafkaConf (..), KafkaProps (..), TopicConf (..), TopicProps (..), getKafkaConf, getRdKafka, kafkaConf, topicConf)
import Kafka.Internal.Shared (kafkaErrorToMaybe, maybeToLeft, rdKafkaErrorToEither, setSaslCredentials')

import Kafka.Consumer.ConsumerProperties as X
import Kafka.Consumer.Subscription as X
import Kafka.Consumer.Types as X hiding (KafkaConsumer)
import Kafka.Types as X

-- | Runs high-level kafka consumer.
-- A callback provided is expected to call 'pollMessage' when convenient.
{-# DEPRECATED runConsumer "Use 'newConsumer'/'closeConsumer' instead" #-}
runConsumer ::
  ConsumerProperties ->
  Subscription ->
  -- | A callback function to poll and handle messages
  (KafkaConsumer -> IO (Either KafkaError a)) ->
  IO (Either KafkaError a)
runConsumer cp sub f =
  bracket mkConsumer clConsumer runHandler
  where
    mkConsumer = newConsumer cp sub

    clConsumer (Left err) = return (Left err)
    clConsumer (Right kc) = maybeToLeft <$> closeConsumer kc

    runHandler (Left err) = return (Left err)
    runHandler (Right kc) = f kc

-- | Create a `KafkaConsumer`. This consumer must be correctly released using 'closeConsumer'.
newConsumer ::
  (MonadIO m) =>
  ConsumerProperties ->
  Subscription ->
  m (Either KafkaError KafkaConsumer)
newConsumer props (Subscription ts tp) = liftIO $ do
  let cp = case cpCallbackPollMode props of
        CallbackPollModeAsync -> setCallback (rebalanceCallback (\_ _ -> return ())) <> props
        CallbackPollModeSync -> props
  kc@(KafkaConf kc' qref _) <- newConsumerConf cp
  tp' <- topicConf (TopicProps tp)
  _ <- setDefaultTopicConf kc tp'
  rdk <- newRdKafkaT RdKafkaConsumer kc'
  case rdk of
    Left err ->
      return $
        Left (KafkaError err)
    Right rdk' -> do
      let kafka =
            KafkaConsumer (Kafka rdk') kc

      when (cpCallbackPollMode props == CallbackPollModeAsync) $ do
        messageQueue <- rdKafkaQueueNew rdk'
        writeIORef qref (Just messageQueue)

      redErr <- redirectCallbacksPoll kafka
      case redErr of
        Just err -> do
          _ <- closeConsumer kafka
          return (Left err)
        Nothing -> do
          forM_ (cpLogLevel cp) $
            setConsumerLogLevel kafka

          subscribeError <-
            if Set.null ts
              then
                pure Nothing
              else
                subscribe kafka ts

          case subscribeError of
            Nothing -> do
              when (cpCallbackPollMode props == CallbackPollModeAsync) $
                runConsumerLoop kafka (Just $ Timeout 100)
              return (Right kafka)
            Just err -> do
              _ <- closeConsumer kafka
              return (Left err)

-- | Polls a single message
--
-- If a /fatal/ error has been raised on this client, this returns
-- @Left ('KafkaResponseError' 'RdKafkaRespErrFatal')@ immediately, without
-- polling, and keeps doing so on every subsequent call. A fatal error is
-- permanent — librdkafka halts all consumer group activity for the client and
-- the only remedy is to close the consumer — so it is reported until the
-- consumer is closed rather than delivered once and forgotten.
--
-- This matters most in 'CallbackPollModeAsync'. There the application drains a
-- side queue, while consumer errors — including the fatal one — are delivered
-- only on librdkafka's consumer queue, which the background callback-poll loop
-- drains. Before this check existed, a fatal error was therefore unobservable
-- in async mode: the application simply polled an empty side queue forever. It
-- could not be observed through @error_cb@ either, because librdkafka returns
-- consumer errors to the application as messages and never routes them to the
-- error callback.
--
-- Use 'consumerFatalError' to recover the underlying cause of the fatal error
-- and its human readable description.
pollMessage ::
  (MonadIO m) =>
  KafkaConsumer ->
  -- | the timeout, in milliseconds
  Timeout ->
  -- | Left on error or timeout, right for success
  m (Either KafkaError (ConsumerRecord (Maybe BS.ByteString) (Maybe BS.ByteString)))
pollMessage c@(KafkaConsumer _ (KafkaConf _ qr _)) (Timeout ms) =
  liftIO $
    pollFatalError c >>= \case
      Just err -> pure (Left err)
      Nothing -> do
        mbq <- readIORef qr
        case mbq of
          Nothing -> rdKafkaConsumerPoll (getRdKafka c) ms >>= fromMessagePtr
          Just q -> rdKafkaConsumeQueue q ms >>= fromMessagePtr

-- | Polls up to 'BatchSize' messages.
-- Unlike 'pollMessage' this function does not return usual "timeout" errors.
-- An empty batch is returned when there are no messages available.
--
-- This API is not available when 'CallbackPollMode' is set to 'CallbackPollModeSync'.
--
-- As with 'pollMessage', if a fatal error has been raised on this client this
-- returns a single-element batch
-- @['Left' ('KafkaResponseError' 'RdKafkaRespErrFatal')]@ immediately and keeps
-- doing so on every subsequent call. Note that this is the one error this
-- function does return: an ordinary empty batch still means "no messages
-- available", but a fatal error means the consumer is permanently dead.
pollMessageBatch ::
  (MonadIO m) =>
  KafkaConsumer ->
  Timeout ->
  BatchSize ->
  m [Either KafkaError (ConsumerRecord (Maybe BS.ByteString) (Maybe BS.ByteString))]
pollMessageBatch c@(KafkaConsumer _ (KafkaConf _ qr _)) (Timeout ms) (BatchSize b) =
  liftIO $
    pollFatalError c >>= \case
      Just err -> pure [Left err]
      Nothing -> do
        pollConsumerEvents c Nothing
        mbq <- readIORef qr
        case mbq of
          Nothing -> return [Left $ KafkaBadSpecification "Calling pollMessageBatch while CallbackPollMode is set to CallbackPollModeSync."]
          Just q -> whileNoCallbackRunning c $ rdKafkaConsumeBatchQueue q ms b >>= traverse fromMessagePtr

-- | Whether the current assignment was lost involuntarily
-- (@rd_kafka_assignment_lost@): the consumer was evicted from the group, for
-- example after its session timed out, rather than revoked by a normal
-- rebalance. Meaningful inside a rebalance callback handling a revocation, where
-- offsets for a lost assignment must not be committed.
assignmentLost :: (MonadIO m) => KafkaConsumer -> m Bool
assignmentLost c = liftIO $ rdKafkaAssignmentLost (getRdKafka c)

-- | Whether 'closeConsumer' has been called on this consumer
-- (@rd_kafka_consumer_closed@).
consumerClosed :: (MonadIO m) => KafkaConsumer -> m Bool
consumerClosed c = liftIO $ rdKafkaConsumerClosed (getRdKafka c)

-- | Replaces the SASL username and password the consumer uses for future
-- authentications (@rd_kafka_sasl_set_credentials@); existing connections
-- are not re-authenticated. librdkafka stores the credentials whether or not
-- SASL is configured.
setSaslCredentials ::
  (MonadIO m) =>
  KafkaConsumer ->
  -- | username
  Text ->
  -- | password
  Text ->
  m (Maybe KafkaError)
setSaslCredentials c username password = liftIO $ setSaslCredentials' c username password

-- | Returns the underlying cause of the fatal error raised on this client,
-- together with librdkafka's human readable description of it, or 'Nothing'
-- when no fatal error has been raised.
--
-- Fatal errors are permanent. They are raised for conditions from which the
-- client cannot recover — the canonical consumer example is a /fenced/ static
-- group member, where a second consumer joined with the same
-- @group.instance.id@ and the broker fenced this one. Once a fatal error is
-- raised librdkafka halts all consumer group activity: rejoins return
-- immediately and subscribes are treated as unsubscribes. The only remedy is to
-- close this consumer and create a new one.
--
-- 'pollMessage' and 'pollMessageBatch' report the /generic/ code
-- 'RdKafkaRespErrFatal' in-band, identically in both callback poll modes; this
-- function is how the specific cause — for example
-- @RdKafkaRespErrFencedInstanceId@ — is recovered for logging or alerting.
consumerFatalError :: (MonadIO m) => KafkaConsumer -> m (Maybe (KafkaError, Text))
consumerFatalError c = liftIO $ do
  (cause, description) <- rdKafkaFatalError (getRdKafka c)
  pure $ case cause of
    RdKafkaRespErrNoError -> Nothing
    _ -> Just (KafkaResponseError cause, description)

-- | Commit message's offset on broker for the message's partition.
commitOffsetMessage ::
  (MonadIO m) =>
  OffsetCommit ->
  KafkaConsumer ->
  ConsumerRecord k v ->
  m (Maybe KafkaError)
commitOffsetMessage o k m =
  liftIO $ toNativeTopicPartitionList [topicPartitionFromMessageForCommit m] >>= commitOffsets o k

-- | Stores message's offset locally for the message's partition.
storeOffsetMessage ::
  (MonadIO m) =>
  KafkaConsumer ->
  ConsumerRecord k v ->
  m (Maybe KafkaError)
storeOffsetMessage k m =
  liftIO $ toNativeTopicPartitionList [topicPartitionFromMessageForCommit m] >>= commitOffsetsStore k

-- | Stores offsets locally
storeOffsets ::
  (MonadIO m) =>
  KafkaConsumer ->
  [TopicPartition] ->
  m (Maybe KafkaError)
storeOffsets k ps =
  liftIO $ toNativeTopicPartitionList ps >>= commitOffsetsStore k

-- | Commit offsets for all currently assigned partitions.
commitAllOffsets ::
  (MonadIO m) =>
  OffsetCommit ->
  KafkaConsumer ->
  m (Maybe KafkaError)
commitAllOffsets o k =
  liftIO $ newForeignPtr_ nullPtr >>= commitOffsets o k

-- | Commit offsets for all currently assigned partitions.
commitPartitionsOffsets ::
  (MonadIO m) =>
  OffsetCommit ->
  KafkaConsumer ->
  [TopicPartition] ->
  m (Maybe KafkaError)
commitPartitionsOffsets o k ps =
  liftIO $ toNativeTopicPartitionList ps >>= commitOffsets o k

-- | Assigns the consumer to consume from the given topics, partitions,
-- and offsets.
assign :: (MonadIO m) => KafkaConsumer -> [TopicPartition] -> m (Maybe KafkaError)
assign (KafkaConsumer (Kafka k) _) ps = liftIO $ do
  tps <- toNativeTopicPartitionList ps
  kafkaErrorToMaybe . KafkaResponseError <$> rdKafkaAssign k tps

-- | Returns current consumer's assignment
assignment :: (MonadIO m) => KafkaConsumer -> m (Either KafkaError (M.Map TopicName [PartitionId]))
assignment (KafkaConsumer (Kafka k) _) = liftIO $ do
  tpl <- rdKafkaAssignment k
  tps <- traverse fromNativeTopicPartitionList'' (left KafkaResponseError tpl)
  return $ tpMap <$> tps
  where
    tpMap ts = toMap $ (tpTopicName &&& tpPartition) <$> ts

-- | Returns current consumer's subscription
subscription :: (MonadIO m) => KafkaConsumer -> m (Either KafkaError [(TopicName, SubscribedPartitions)])
subscription (KafkaConsumer (Kafka k) _) = liftIO $ do
  tpl <- rdKafkaSubscription k
  tps <- traverse fromNativeTopicPartitionList'' (left KafkaResponseError tpl)
  return $ toSub <$> tps
  where
    toSub ts = M.toList $ subParts <$> tpMap ts
    tpMap ts = toMap $ (tpTopicName &&& tpPartition) <$> ts
    subParts [PartitionId (-1)] = SubscribedPartitionsAll
    subParts ps = SubscribedPartitions ps

-- | Pauses specified partitions on the current consumer.
pausePartitions :: (MonadIO m) => KafkaConsumer -> [(TopicName, PartitionId)] -> m KafkaError
pausePartitions (KafkaConsumer (Kafka k) _) ps = liftIO $ do
  pl <- newRdKafkaTopicPartitionListT (length ps)
  mapM_ (\(TopicName topicName, PartitionId partitionId) -> rdKafkaTopicPartitionListAdd pl (Text.unpack topicName) partitionId) ps
  KafkaResponseError <$> rdKafkaPausePartitions k pl

-- | Resumes specified partitions on the current consumer.
resumePartitions :: (MonadIO m) => KafkaConsumer -> [(TopicName, PartitionId)] -> m KafkaError
resumePartitions (KafkaConsumer (Kafka k) _) ps = liftIO $ do
  pl <- newRdKafkaTopicPartitionListT (length ps)
  mapM_ (\(TopicName topicName, PartitionId partitionId) -> rdKafkaTopicPartitionListAdd pl (Text.unpack topicName) partitionId) ps
  KafkaResponseError <$> rdKafkaResumePartitions k pl

-- | Seek a particular offset for each provided 'TopicPartition'
{-# DEPRECATED seek "Use seekPartitions instead" #-}
seek :: (MonadIO m) => KafkaConsumer -> Timeout -> [TopicPartition] -> m (Maybe KafkaError)
seek (KafkaConsumer (Kafka k) _) (Timeout timeout) tps =
  liftIO $
    either Just (const Nothing) <$> seekAll
  where
    seekAll = runExceptT $ do
      tr <- traverse (ExceptT . topicPair) tps
      mapM_ (\(kt, p, o) -> ExceptT (rdSeek kt p o)) tr

    rdSeek kt (PartitionId p) o =
      rdKafkaErrorToEither <$> rdKafkaSeek kt (fromIntegral p) (offsetToInt64 o) timeout

    topicPair tp = do
      let (TopicName tn) = tpTopicName tp
      nt <- newRdKafkaTopicT k (Text.unpack tn) Nothing
      return $ bimap KafkaError (,tpPartition tp,tpOffset tp) (first Text.pack nt)

-- | Seek consumer for partitions in partitions to the per-partition
--    offset in the offset field of partitions.
seekPartitions :: (MonadIO m) => KafkaConsumer -> [TopicPartition] -> Timeout -> m (Maybe KafkaError)
seekPartitions (KafkaConsumer (Kafka k) _) ps (Timeout timeout) = liftIO $ do
  tps <- toNativeTopicPartitionList ps
  err <- bracket (rdKafkaSeekPartitions k tps timeout) rdKafkaErrorDestroy rdKafkaErrorCode
  pure $ either Just (const Nothing) $ rdKafkaErrorToEither err

-- | Retrieve committed offsets for topics+partitions.
committed :: (MonadIO m) => KafkaConsumer -> Timeout -> [(TopicName, PartitionId)] -> m (Either KafkaError [TopicPartition])
committed (KafkaConsumer (Kafka k) _) (Timeout timeout) tps = liftIO $ do
  ntps <- toNativeTopicPartitionList' tps
  res <- rdKafkaCommitted k ntps timeout
  case res of
    RdKafkaRespErrNoError -> Right <$> fromNativeTopicPartitionList'' ntps
    err -> return $ Left (KafkaResponseError err)

-- | Retrieve current positions (last consumed message offset+1) for the current running instance of the consumer.
-- If the current consumer hasn't received any messages for a given partition, 'PartitionOffsetInvalid' is returned.
position :: (MonadIO m) => KafkaConsumer -> [(TopicName, PartitionId)] -> m (Either KafkaError [TopicPartition])
position (KafkaConsumer (Kafka k) _) tps = liftIO $ do
  ntps <- toNativeTopicPartitionList' tps
  res <- rdKafkaPosition k ntps
  case res of
    RdKafkaRespErrNoError -> Right <$> fromNativeTopicPartitionList'' ntps
    err -> return $ Left (KafkaResponseError err)

-- | Polls the provided kafka consumer for events.
--
-- Events will cause application provided callbacks to be called.
--
-- The 'Timeout' argument specifies the maximum amount of time
-- (in milliseconds) that the call will block waiting for events.
--
-- This function is called on each 'pollMessage' and, if runtime allows
-- multi threading, it is called periodically in a separate thread
-- to ensure the callbacks are handled ASAP.
--
-- There is no particular need to call this function manually
-- unless some special cases in a single-threaded environment
-- when polling for events on each 'pollMessage' is not
-- frequent enough.
--
-- Any message librdkafka returns from this poll (a fetched record that raced a
-- partition-queue redirect, or a consumer error) is discarded and destroyed.
-- Fatal errors remain observable through 'pollMessage', 'pollMessageBatch' and
-- 'consumerFatalError', which read librdkafka's fatal state directly.
pollConsumerEvents :: KafkaConsumer -> Maybe Timeout -> IO ()
pollConsumerEvents k timeout =
  void . withCallbackPollEnabled k $ pollConsumerEvents' k timeout

-- | Closes the consumer.
--
-- See 'newConsumer'
closeConsumer :: (MonadIO m) => KafkaConsumer -> m (Maybe KafkaError)
closeConsumer (KafkaConsumer (Kafka k) (KafkaConf _ qr statusVar)) = liftIO $
  -- because closing the consumer will raise callbacks,
  -- prevent the async loop from doing it at the same time
  modifyMVar statusVar $ \_ -> do
    -- librdkafka says:
    --   Prior to destroying the client instance, loose your reference to the
    --   background queue by calling rd_kafka_queue_destroy()
    readIORef qr >>= mapM_ rdKafkaQueueDestroy
    res <- kafkaErrorToMaybe . KafkaResponseError <$> rdKafkaConsumerClose k
    pure (CallbackPollDisabled, res)

-- | Rewind consumer's consume position to the last committed offsets for the current assignment.
-- NOTE: follows https://github.com/edenhill/librdkafka/blob/master/examples/transactions.c#L166
rewindConsumer ::
  (MonadIO m) =>
  KafkaConsumer ->
  Timeout ->
  m (Maybe KafkaError)
rewindConsumer c to = liftIO $ do
  ret <- assignment c
  case ret of
    Left err -> pure $ Just err
    Right os -> do
      if M.size os == 0
        -- No current assignment to rewind
        then pure Nothing
        else do
          let tps = foldr (\(t, ps) acc -> map (t,) ps ++ acc) [] $ M.toList os
          ret' <- committed c to tps
          case ret' of
            Left err -> pure $ Just err
            Right ps -> do
              -- Seek to committed offset, or start of partition if no
              -- committed offset is available.
              let ps' = map checkOffsets ps
              seekPartitions c ps' to
  where
    checkOffsets :: TopicPartition -> TopicPartition
    checkOffsets tp
      | isUncommitedOffset $ tpOffset tp =
          tp {tpOffset = PartitionOffsetBeginning}
      | otherwise = tp

    isUncommitedOffset :: PartitionOffset -> Bool
    isUncommitedOffset (PartitionOffset _) = False
    isUncommitedOffset _ = True

-----------------------------------------------------------------------------
newConsumerConf :: ConsumerProperties -> IO KafkaConf
newConsumerConf ConsumerProperties {cpProps = m, cpCallbacks = cbs} = do
  conf <- kafkaConf (KafkaProps m)
  forM_ cbs (\(Callback setCb) -> setCb conf)
  return conf

-- | Subscribes to a given list of topics.
--
-- Wildcard (regex) topics are supported by the /librdkafka/ assignor:
-- any topic name in the topics list that is prefixed with @^@ will
-- be regex-matched to the full list of topics in the cluster and matching
-- topics will be added to the subscription list.
subscribe :: KafkaConsumer -> Set TopicName -> IO (Maybe KafkaError)
subscribe (KafkaConsumer (Kafka k) _) ts = do
  pl <- newRdKafkaTopicPartitionListT (length ts)
  mapM_ (\(TopicName t) -> rdKafkaTopicPartitionListAdd pl (Text.unpack t) (-1)) (Set.toList ts)
  res <- KafkaResponseError <$> rdKafkaSubscribe k pl
  return $ kafkaErrorToMaybe res

setDefaultTopicConf :: KafkaConf -> TopicConf -> IO ()
setDefaultTopicConf (KafkaConf kc _ _) (TopicConf tc) =
  rdKafkaTopicConfDup tc >>= rdKafkaConfSetDefaultTopicConf kc

commitOffsets :: OffsetCommit -> KafkaConsumer -> RdKafkaTopicPartitionListTPtr -> IO (Maybe KafkaError)
commitOffsets o (KafkaConsumer (Kafka k) _) pl =
  kafkaErrorToMaybe . KafkaResponseError <$> rdKafkaCommit k pl (offsetCommitToBool o)

commitOffsetsStore :: KafkaConsumer -> RdKafkaTopicPartitionListTPtr -> IO (Maybe KafkaError)
commitOffsetsStore (KafkaConsumer (Kafka k) _) pl =
  kafkaErrorToMaybe . KafkaResponseError <$> rdKafkaOffsetsStore k pl

setConsumerLogLevel :: KafkaConsumer -> KafkaLogLevel -> IO ()
setConsumerLogLevel (KafkaConsumer (Kafka k) _) level =
  liftIO $ rdKafkaSetLogLevel k (fromEnum level)

redirectCallbacksPoll :: KafkaConsumer -> IO (Maybe KafkaError)
redirectCallbacksPoll (KafkaConsumer (Kafka k) _) =
  kafkaErrorToMaybe . KafkaResponseError <$> rdKafkaPollSetConsumer k

runConsumerLoop :: KafkaConsumer -> Maybe Timeout -> IO ()
runConsumerLoop k timeout =
  when rtsSupportsBoundThreads $ void $ forkIO go
  where
    go = do
      st <- withCallbackPollEnabled k (pollConsumerEvents' k timeout)
      case st of
        CallbackPollEnabled -> go
        CallbackPollDisabled -> pure ()

whileNoCallbackRunning :: KafkaConsumer -> IO a -> IO a
whileNoCallbackRunning k f = do
  let statusVar = kcfgCallbackPollStatus (getKafkaConf k)
  withMVar statusVar $ \_ -> f

withCallbackPollEnabled :: KafkaConsumer -> IO () -> IO CallbackPollStatus
withCallbackPollEnabled k f = do
  let statusVar = kcfgCallbackPollStatus (getKafkaConf k)
  withMVar statusVar $ \case
    CallbackPollEnabled -> f >> pure CallbackPollEnabled
    CallbackPollDisabled -> pure CallbackPollDisabled

-- | Polls the consumer queue to let librdkafka serve callback-type events, and
-- destroys whatever message that poll hands back.
--
-- Most events on the consumer queue are served as callbacks inside librdkafka
-- during the poll itself and nothing is returned. Two kinds of event are
-- instead /returned/ to the caller as a message: a fetched record that raced a
-- partition-queue redirect, and any consumer error. This function is only
-- interested in the callback side effects, so it discards both — but it must
-- still free them. The returned foreign pointer carries no finalizer, so
-- dropping it without calling 'rdKafkaMessageDestroy' leaks the underlying C
-- message; before this was fixed, every consumer error and every raced record
-- leaked, on a loop that runs every 100 ms.
--
-- Discarding a returned consumer error costs no observability: fatal errors are
-- reported in-band by 'pollMessage' and 'pollMessageBatch', which read
-- librdkafka's fatal state directly rather than relying on this loop having
-- seen the message, and non-fatal consumer errors were never visible from here
-- to begin with (librdkafka does not route them to @error_cb@).
pollConsumerEvents' :: KafkaConsumer -> Maybe Timeout -> IO ()
pollConsumerEvents' k timeout = do
  let (Timeout tm) = fromMaybe (Timeout 0) timeout
  msgPtr <- rdKafkaConsumerPoll (getRdKafka k) tm
  withForeignPtr msgPtr $ \realPtr ->
    unless (realPtr == nullPtr) (rdKafkaMessageDestroy realPtr)

-- | The in-band error a poll must report because a fatal error has been raised
-- on this client, or 'Nothing' when the client is healthy.
--
-- Reports the generic 'RdKafkaRespErrFatal' rather than the underlying cause so
-- that both callback poll modes deliver an identical value: in
-- 'CallbackPollModeSync' librdkafka itself hands exactly this code to the
-- application as a consumer error, and callers should not have to discriminate
-- by poll mode when classifying an error as fatal. 'consumerFatalError' recovers
-- the cause when it is wanted.
pollFatalError :: KafkaConsumer -> IO (Maybe KafkaError)
pollFatalError c = do
  (cause, _description) <- rdKafkaFatalError (getRdKafka c)
  pure $ case cause of
    RdKafkaRespErrNoError -> Nothing
    _ -> Just (KafkaResponseError RdKafkaRespErrFatal)
