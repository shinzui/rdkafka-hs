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
    newAssignedConsumer,
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
    pollMessageAttributed,
    pollConsumerEvents,
    pollMessageBatch,
    consumerFatalError,
    assignmentLost,
    consumerClosed,
    setConsumerQueueIOEvent,
    setSaslCredentials,
    commitOffsetMessage,
    commitAllOffsets,
    commitPartitionsOffsets,
    storeOffsets,
    storeOffsetMessage,
    rewindConsumer,
    closeConsumer,
    destroyConsumer,
    -- ReExport Types
    RdKafkaRespErrT (..),
  )
where

import Kafka.Internal.CallbackOwner (releaseUnattachedCallbacks)

import Control.Arrow (left, (&&&))
import Control.Concurrent (forkIO, modifyMVar, modifyMVarMasked_, rtsSupportsBoundThreads, withMVar)
import Control.Exception (bracket, finally, mask, mask_, onException)
import Control.Monad (forM_, unless, void, when)
import Control.Monad.IO.Class (MonadIO (liftIO))
import Control.Monad.Trans.Except (ExceptT (ExceptT), runExceptT)
import Data.Bifunctor (bimap, first)
import Data.ByteString qualified as BS
import Data.ByteString.Unsafe (unsafeUseAsCStringLen)
import Data.IORef (IORef, atomicModifyIORef', readIORef, writeIORef)
import Data.List (find)
import Data.Map qualified as M hiding (foldr, map)
import Data.Maybe (fromMaybe, isJust)
import Data.Sequence (ViewL (..), (|>))
import Data.Sequence qualified as Seq
import Data.Set (Set)
import Data.Set qualified as Set
import Data.Text (Text)
import Data.Text qualified as Text
import Foreign hiding (void)
import Kafka.Consumer.Callbacks (forwardPartitionsToMessageQueue)
import Kafka.Consumer.Convert (copyMessagePtr, copyMessagePtrAttributed, fromMessagePtr, fromMessagePtrAttributed, fromNativeTopicPartitionList'', nativeTopicPartitionErrors, offsetCommitToBool, offsetToInt64, toMap, toNativeTopicPartitionList, toNativeTopicPartitionList', topicPartitionFromMessageForCommit, withPolledMessage)
import Kafka.Consumer.Types (KafkaConsumer (..))
import Kafka.Internal.RdKafka (RdKafkaQueueTPtr, RdKafkaRespErrT (..), RdKafkaTPtr, RdKafkaTopicPartitionListTPtr, RdKafkaTypeT (..), newRdKafkaT, newRdKafkaTopicPartitionListT, newRdKafkaTopicT, rdKafkaAssign, rdKafkaAssignment, rdKafkaAssignmentLost, rdKafkaCommit, rdKafkaCommitted, rdKafkaConsumeBatchQueue, rdKafkaConsumeQueue, rdKafkaConsumerClose, rdKafkaConsumerClosed, rdKafkaConsumerPoll, rdKafkaErrorCode, rdKafkaErrorDestroy, rdKafkaFatalError, rdKafkaMessageDestroy, rdKafkaOffsetsStore, rdKafkaPausePartitions, rdKafkaPollSetConsumer, rdKafkaPosition, rdKafkaQueueDestroy, rdKafkaQueueGetConsumer, rdKafkaQueueIoEventEnable, rdKafkaQueueNew, rdKafkaResumePartitions, rdKafkaSeek, rdKafkaSeekPartitions, rdKafkaSetLogLevel, rdKafkaSubscribe, rdKafkaSubscription, rdKafkaTopicPartitionListAdd)
import Kafka.Internal.Setup (Callback (..), CallbackPollStatus (..), ClientLifecycle (..), Kafka (..), KafkaConf (..), KafkaProps (..), PendingMessage (..), PendingMessages (..), PendingTimestamp (..), TopicProps (..), discardPendingMessages, emptyPendingMessages, getKafkaConf, getRdKafka, getRdMsgQueue, kafkaConf, pendingMessagePartition, resetPendingMessages, setAllKafkaConfValues, topicConf)
import Kafka.Internal.Shared (kafkaErrorToMaybe, maybeToLeft, rdKafkaErrorToEither, setSaslCredentials', validateTopicPartitions)
import System.Posix.Types (Fd (..))

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
--
-- With an empty subscription (@mempty@) the consumer never calls
-- @rd_kafka_subscribe@ and so never joins its consumer group; it reads only the
-- partitions given to 'assign'. 'newAssignedConsumer' names that case.
--
-- Async callback polling (the default) requires an executable linked with
-- @-threaded@. Without that runtime, choose 'CallbackPollModeSync'; requesting
-- async polling returns 'KafkaBadSpecification' before allocating a client.
newConsumer ::
  (MonadIO m) =>
  ConsumerProperties ->
  Subscription ->
  m (Either KafkaError KafkaConsumer)
newConsumer props (Subscription ts tp)
  | effectiveCallbackPollMode props == CallbackPollModeAsync && not rtsSupportsBoundThreads =
      pure (Left (KafkaBadSpecification "Async callback polling requires -threaded; select CallbackPollModeSync for a nonthreaded runtime."))
  | Just failure <- validateTopicPartitions [(topic, PartitionId 0) | topic <- Set.toList ts] = pure (Left failure)
  | otherwise = liftIO $ mask $ \restore -> do
      -- Both modes track rebalance invalidation. A later user callback replaces
      -- the no-op handler while retaining the same checked state transitions.
      let cp = setCallback (rebalanceCallback (\_ _ -> return ())) <> props
      kc@(KafkaConf kc' qref _ _ _ _) <- newConsumerConf cp
      flip onException (releaseUnattachedCallbacks (kcfgCallbackOwner kc)) $ restore $ do
        -- Validate subscription values as topic properties, then merge only their
        -- explicit keys so globally supplied topic settings remain effective.
        _ <- topicConf (TopicProps tp)
        setAllKafkaConfValues kc (KafkaProps tp)
        rdk <- newRdKafkaT (kcfgCallbackOwner kc) RdKafkaConsumer kc'
        case rdk of
          Left err ->
            return $
              Left (KafkaError err)
          Right rdk' -> do
            let kafka =
                  KafkaConsumer (Kafka rdk') kc

            flip onException (destroyConsumer kafka) $ do
              when (effectiveCallbackPollMode props == CallbackPollModeAsync) $ mask_ $ do
                messageQueue <- rdKafkaQueueNew rdk'
                writeIORef qref (Just messageQueue)

              redErr <- redirectCallbacksPoll kafka
              case redErr of
                Just err -> do
                  _ <- closeConsumer kafka `finally` destroyConsumer kafka
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
                      when (effectiveCallbackPollMode props == CallbackPollModeAsync) $
                        runConsumerLoop kafka (Just $ Timeout 100)
                      return (Right kafka)
                    Just err -> do
                      _ <- closeConsumer kafka `finally` destroyConsumer kafka
                      return (Left err)

-- | Create a consumer that never subscribes and therefore never joins a group:
-- it reads only the partitions given to 'assign', from the offsets given there.
--
-- librdkafka still requires a @group.id@. With 'noAutoCommit', and without
-- committing offsets explicitly, the consumer sends neither JoinGroup nor
-- OffsetCommit, so the broker never records the group. Release the consumer with
-- 'closeConsumer' or 'destroyConsumer'.
newAssignedConsumer :: (MonadIO m) => ConsumerProperties -> m (Either KafkaError KafkaConsumer)
newAssignedConsumer props = newConsumer props mempty

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
pollMessage c@(KafkaConsumer _ (KafkaConf _ qr _ _ _ _)) (Timeout ms) =
  liftIO $ withForeignPtr (getRdKafka c) $ \_ ->
    pollFatalError c >>= \case
      Just err -> pure (Left err)
      Nothing -> do
        servePendingCallbacks c
        nextPendingMessage c >>= \case
          Just message -> pure (first cmeError message)
          Nothing -> do
            mbq <- readIORef qr
            case mbq of
              Nothing -> fromMessagePtr (rdKafkaConsumerPoll (getRdKafka c) ms)
              Just q -> fromMessagePtr (rdKafkaConsumeQueue q ms)

-- | Like 'pollMessage', but an error comes with the topic, partition and offset
-- librdkafka attached to the errored message, so an application can tell, for
-- example, which partition a 'RdKafkaRespErrPartitionEof' belongs to.
--
-- A raised fatal error is reported first, as by 'pollMessage', as
-- @'ConsumerMessageError' ('KafkaResponseError' 'RdKafkaRespErrFatal')@ with
-- no origin; so is a poll timeout, as 'RdKafkaRespErrTimedOut'.
pollMessageAttributed ::
  (MonadIO m) =>
  KafkaConsumer ->
  -- | the timeout, in milliseconds
  Timeout ->
  m (Either ConsumerMessageError (ConsumerRecord (Maybe BS.ByteString) (Maybe BS.ByteString)))
pollMessageAttributed c@(KafkaConsumer _ (KafkaConf _ qr _ _ _ _)) (Timeout ms) =
  liftIO $ withForeignPtr (getRdKafka c) $ \_ ->
    pollFatalError c >>= \case
      Just err -> pure (Left (ConsumerMessageError err Nothing Nothing Nothing))
      Nothing -> do
        servePendingCallbacks c
        nextPendingMessage c >>= \case
          Just message -> pure message
          Nothing -> do
            mbq <- readIORef qr
            case mbq of
              Nothing -> fromMessagePtrAttributed (rdKafkaConsumerPoll (getRdKafka c) ms)
              Just q -> fromMessagePtrAttributed (rdKafkaConsumeQueue q ms)

-- | Polls up to 'BatchSize' messages.
-- A normal timeout returns an empty batch; native acquisition failures and
-- per-record decoding failures remain errors. Zero size
-- returns an empty batch; negative or unrepresentable sizes return a bad
-- specification. Native acquisition and message decoding errors remain errors.
--
-- This API is not available when 'CallbackPollMode' is set to 'CallbackPollModeSync'.
--
-- As with 'pollMessage', if a fatal error has been raised on this client this
-- returns a single-element batch
-- @['Left' ('KafkaResponseError' 'RdKafkaRespErrFatal')]@ immediately and keeps
-- doing so on every subsequent call. This fatal path is distinct from an
-- ordinary empty batch, which means "no messages
-- available", but a fatal error means the consumer is permanently dead.
pollMessageBatch ::
  (MonadIO m) =>
  KafkaConsumer ->
  Timeout ->
  BatchSize ->
  m [Either KafkaError (ConsumerRecord (Maybe BS.ByteString) (Maybe BS.ByteString))]
pollMessageBatch c@(KafkaConsumer _ (KafkaConf _ qr _ _ _ _)) (Timeout ms) (BatchSize b) =
  liftIO $ withForeignPtr (getRdKafka c) $ \_ ->
    pollFatalError c >>= \case
      Just err -> pure [Left err]
      Nothing -> do
        mbq <- readIORef qr
        case mbq of
          Nothing -> return [Left $ KafkaBadSpecification "Calling pollMessageBatch while CallbackPollMode is set to CallbackPollModeSync."]
          Just q
            | b < 0 || b > maxBound `div` sizeOf (nullPtr :: Ptr ()) ->
                pure [Left (KafkaBadSpecification "Batch size must be nonnegative and fit a native pointer array.")]
            | b == 0 -> pure []
            | otherwise -> do
                pollConsumerEvents c Nothing
                whileNoCallbackRunning c $
                  bracket (rdKafkaConsumeBatchQueue q ms b) releaseBatch $ \case
                    Left code -> pure [Left (KafkaResponseError code)]
                    Right messages -> traverse copyMessagePtr messages
  where
    releaseBatch (Left _) = pure ()
    releaseBatch (Right messages) = mapM_ (\message -> withForeignPtr message rdKafkaMessageDestroy) messages

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

-- | Asks librdkafka to write a payload to a file descriptor when records or
-- events become available on the queue 'pollMessage' and 'pollMessageBatch'
-- read from, so an application can wait with
-- 'Control.Concurrent.threadWaitRead' instead of polling on a timer
-- (@rd_kafka_queue_io_event_enable@). 'Nothing' disables the notification.
--
-- The queue is the consumer's message queue in 'CallbackPollModeAsync' and
-- librdkafka's consumer queue in 'CallbackPollModeSync'; in both cases it is
-- the final destination of the partition queues, which is where librdkafka
-- requires the event to be enabled. Enabling again replaces the previous
-- descriptor and payload. After 'closeConsumer' this does nothing.
--
-- librdkafka's rules:
--
-- * The descriptor must be non-blocking (for a pipe's write end, set
--   'System.Posix.IO.NonBlockingRead', which sets @O_NONBLOCK@): librdkafka writes from
--   an internal thread and ignores write errors.
-- * IO-event and callback-event notification are mutually exclusive on one queue.
-- * librdkafka copies the payload before this function returns. The descriptor
--   stays owned by the caller: disable the notification (or destroy the
--   consumer) before closing it, or librdkafka may write to a reused descriptor.
--
-- The notification is edge-triggered. librdkafka writes when an op lands in the
-- empty queue, at most once until the queue is next polled, so a wake-up can
-- stand for several records and is not repeated while records remain. It can
-- also stand for something other than a record: a callback-serving event, an
-- error, or a wake-up with an empty queue. Wait like this:
--
-- 1. Enable the notification.
-- 2. Read and discard the pending payload bytes from the descriptor, then poll
--    until the queue is empty (records may have arrived before the notification
--    was enabled, or since the last wake-up).
-- 3. Wait, with a timeout, for the descriptor to become readable, and go back to
--    step 2.
--
-- Discard the bytes before polling, not after: a record that arrives after the
-- last poll writes a fresh wake-up, and discarding it afterwards would leave
-- the application waiting while that record sits in the queue.
--
-- A wake-up is not a promise of a record, and a fatal error need not produce
-- one: in 'CallbackPollModeAsync' a raised fatal error is delivered on
-- librdkafka's consumer queue, not on the message queue this watches. Bound
-- every wait with a timeout and poll (or call 'consumerFatalError') when it
-- expires, so a fatal consumer is noticed; never wait indefinitely.
--
-- Serialize this with 'closeConsumer' and assignment changes: do not call it
-- concurrently with them, nor from a librdkafka callback.
setConsumerQueueIOEvent :: (MonadIO m) => KafkaConsumer -> Maybe (Fd, BS.ByteString) -> m ()
setConsumerQueueIOEvent c@(KafkaConsumer _ (KafkaConf _ qr statusVar _ _ _)) event =
  liftIO $ withMVar statusVar $ \case
    CallbackPollDisabled -> pure ()
    CallbackPollEnabled ->
      readIORef qr >>= \case
        Just messageQueue -> enable messageQueue
        Nothing ->
          bracket (rdKafkaQueueGetConsumer (getRdKafka c)) releaseQueue $ \consumerQueue ->
            withForeignPtr consumerQueue $ \p -> unless (p == nullPtr) (enable consumerQueue)
  where
    enable q = case event of
      Nothing -> rdKafkaQueueIoEventEnable q (-1) nullPtr 0
      Just (Fd fd, payload) ->
        unsafeUseAsCStringLen payload $ \(bytes, size) ->
          rdKafkaQueueIoEventEnable q (fromIntegral fd) (castPtr bytes) (fromIntegral size)
    releaseQueue q = withForeignPtr q $ \p -> unless (p == nullPtr) (rdKafkaQueueDestroy q)

-- | Replaces the SASL username and password the consumer uses for future
-- authentications (@rd_kafka_sasl_set_credentials@); existing connections
-- are not re-authenticated. librdkafka stores the credentials whether or not
-- SASL is configured.
-- Embedded NUL in either credential returns 'KafkaBadSpecification' before
-- changing the native credentials.
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
commitOffsetMessage _ _ record | Just failure <- validateTopicPartitions [(crTopic record, crPartition record)] = pure (Just failure)
commitOffsetMessage o k m =
  liftIO $ withPartitionList k (toNativeTopicPartitionList [topicPartitionFromMessageForCommit m]) (commitOffsets o k)

-- | Stores message's offset locally for the message's partition.
storeOffsetMessage ::
  (MonadIO m) =>
  KafkaConsumer ->
  ConsumerRecord k v ->
  m (Maybe KafkaError)
storeOffsetMessage _ record | Just failure <- validateTopicPartitions [(crTopic record, crPartition record)] = pure (Just failure)
storeOffsetMessage k m =
  liftIO $ withPartitionList k (toNativeTopicPartitionList [topicPartitionFromMessageForCommit m]) (commitOffsetsStore k)

-- | Stores offsets locally
storeOffsets ::
  (MonadIO m) =>
  KafkaConsumer ->
  [TopicPartition] ->
  m (Maybe KafkaError)
storeOffsets _ ps | Just failure <- validateTopicPartitions [(tpTopicName p, tpPartition p) | p <- ps] = pure (Just failure)
storeOffsets k ps =
  liftIO $ withPartitionList k (toNativeTopicPartitionList ps) (commitOffsetsStore k)

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
commitPartitionsOffsets _ _ ps | Just failure <- validateTopicPartitions [(tpTopicName p, tpPartition p) | p <- ps] = pure (Just failure)
commitPartitionsOffsets o k ps =
  liftIO $ withPartitionList k (toNativeTopicPartitionList ps) (commitOffsets o k)

-- | Assigns the consumer to consume from the given topics, partitions,
-- and offsets.
--
-- The new assignment replaces the previous one; an empty list unassigns every
-- partition. In 'CallbackPollModeAsync' each partition's queue is forwarded to
-- the queue 'pollMessage' reads before it is assigned, so no record is fetched
-- onto librdkafka's consumer queue, where the background loop would discard it.
-- Use this on a consumer from 'newAssignedConsumer'; on a subscribed consumer the
-- rebalance callback assigns partitions.
assign :: (MonadIO m) => KafkaConsumer -> [TopicPartition] -> m (Maybe KafkaError)
assign _ ps | Just failure <- validateTopicPartitions [(tpTopicName p, tpPartition p) | p <- ps] = pure (Just failure)
assign c@(KafkaConsumer (Kafka k) conf) ps = liftIO $ withForeignPtr k $ \_ -> mask_ $ bracket (toNativeTopicPartitionList ps) finalizeForeignPtr $ \tps -> do
  let partitions = (tpTopicName &&& tpPartition) <$> ps
  forwardPartitionsToMessageQueue c partitions
  err <- rdKafkaAssign k tps
  when (err == RdKafkaRespErrNoError) (resetPendingMessages conf)
  -- Reassigning a partition stops its fetcher, and librdkafka clears the
  -- partition queue's forwarding when that stop is processed, after
  -- rd_kafka_assign has returned. The stop requests and rd_kafka_assignment's
  -- request are served in order by librdkafka's main thread, so once
  -- rd_kafka_assignment returns the stops are done and the forwarding can be
  -- restored. Records fetched in between wait on the partition queue itself
  -- (librdkafka does not re-forward an application-forwarded queue) and move
  -- with the forward.
  isAsync <- isJust <$> getRdMsgQueue conf
  when (err == RdKafkaRespErrNoError && isAsync) $ do
    bracket (rdKafkaAssignment k) (mapM_ finalizeForeignPtr) (const (pure ()))
    forwardPartitionsToMessageQueue c partitions
  pure (kafkaErrorToMaybe (KafkaResponseError err))

-- | Returns current consumer's assignment
assignment :: (MonadIO m) => KafkaConsumer -> m (Either KafkaError (M.Map TopicName [PartitionId]))
assignment (KafkaConsumer (Kafka k) _) = liftIO $ withForeignPtr k $ \_ -> bracket (rdKafkaAssignment k) (mapM_ finalizeForeignPtr) $ \tpl -> do
  tps <- traverse fromNativeTopicPartitionList'' (left KafkaResponseError tpl)
  return $ tpMap <$> tps
  where
    tpMap ts = toMap $ (tpTopicName &&& tpPartition) <$> ts

-- | Returns current consumer's subscription
subscription :: (MonadIO m) => KafkaConsumer -> m (Either KafkaError [(TopicName, SubscribedPartitions)])
subscription (KafkaConsumer (Kafka k) _) = liftIO $ withForeignPtr k $ \_ -> bracket (rdKafkaSubscription k) (mapM_ finalizeForeignPtr) $ \tpl -> do
  tps <- traverse fromNativeTopicPartitionList'' (left KafkaResponseError tpl)
  return $ toSub <$> tps
  where
    toSub ts = M.toList $ subParts <$> tpMap ts
    tpMap ts = toMap $ (tpTopicName &&& tpPartition) <$> ts
    subParts [PartitionId (-1)] = SubscribedPartitionsAll
    subParts ps = SubscribedPartitions ps

-- | Pauses specified partitions on the current consumer. Reports the overall
-- native error or the first member error; successful members remain paused.
pausePartitions :: (MonadIO m) => KafkaConsumer -> [(TopicName, PartitionId)] -> m KafkaError
pausePartitions consumer ps = changePause consumer ps True rdKafkaPausePartitions

-- | Resumes specified partitions on the current consumer. Successful members
-- resume even when another member reports an error.
resumePartitions :: (MonadIO m) => KafkaConsumer -> [(TopicName, PartitionId)] -> m KafkaError
resumePartitions consumer ps = changePause consumer ps False rdKafkaResumePartitions

changePause :: (MonadIO m) => KafkaConsumer -> [(TopicName, PartitionId)] -> Bool -> (RdKafkaTPtr -> RdKafkaTopicPartitionListTPtr -> IO RdKafkaRespErrT) -> m KafkaError
changePause _ ps _ _ | Just failure <- validateTopicPartitions ps = pure failure
changePause consumer ps paused action = liftIO $ withForeignPtr (getRdKafka consumer) $ \_ -> mask_ $
  bracket (newRdKafkaTopicPartitionListT (length ps)) finalizeForeignPtr $ \list -> do
    mapM_ (\(TopicName topic, PartitionId partition) -> rdKafkaTopicPartitionListAdd list (Text.unpack topic) partition) ps
    code <- action (getRdKafka consumer) list
    when (code == RdKafkaRespErrNoError) $ do
      errors <- nativeTopicPartitionErrors list
      let successful = Set.fromList [partition | (partition, result) <- zip ps errors, result == RdKafkaRespErrNoError]
      atomicModifyIORef' (kcfgPendingMessages (getKafkaConf consumer)) $ \buffer ->
        let states = pendingPausedPartitions buffer
            updated = if paused then Set.union states successful else states `Set.difference` successful
         in (buffer {pendingPausedPartitions = updated}, ())
    fromMaybe (KafkaResponseError RdKafkaRespErrNoError) <$> partitionListError list code

-- | Seek a particular offset for each provided 'TopicPartition'
{-# DEPRECATED seek "Use seekPartitions instead" #-}
seek :: (MonadIO m) => KafkaConsumer -> Timeout -> [TopicPartition] -> m (Maybe KafkaError)
seek _ _ ps | Just failure <- validateTopicPartitions [(tpTopicName p, tpPartition p) | p <- ps] = pure (Just failure)
seek consumer@(KafkaConsumer (Kafka k) _) (Timeout timeout) tps =
  liftIO $ withForeignPtr k $ \_ ->
    either Just (const Nothing) <$> withTopics tps seekAll
  where
    -- Acquire all topics before seeking, preserving the deprecated wrapper's
    -- error order, and release every topic before returning to its caller.
    withTopics [] action = action []
    withTopics (tp : rest) action =
      topicPair tp >>= \case
        Left err -> pure (Left err)
        Right owned ->
          bracket (pure owned) (\(ptr, _, _, _) -> finalizeForeignPtr ptr) $ \_ ->
            withTopics rest (\remaining -> action (owned : remaining))

    seekAll pairs =
      runExceptT $
        mapM_ (\(kt, topic, p, o) -> ExceptT (rdSeek kt topic p o)) pairs

    rdSeek kt topic partition@(PartitionId p) o = mask_ $ do
      result <- rdKafkaSeek kt (fromIntegral p) (offsetToInt64 o) timeout
      when (result == RdKafkaRespErrNoError) (discardPendingMessages (getKafkaConf consumer) (Set.singleton (topic, partition)))
      pure (rdKafkaErrorToEither result)

    topicPair tp = do
      let (TopicName tn) = tpTopicName tp
      nt <- newRdKafkaTopicT k (Text.unpack tn) Nothing
      return $ bimap KafkaError (,tpTopicName tp,tpPartition tp,tpOffset tp) (first Text.pack nt)

-- | Seek already assigned partitions to their requested offsets. A nonzero
-- timeout waits for the fetcher update and discards prefetched records from the
-- old position. The overall native error takes precedence; otherwise the first
-- failed partition in input order is returned. Successful members of a mixed
-- request are not rolled back when another member fails.
--
-- A zero timeout initiates the operation without waiting for completion. Pending
-- member markers are ignored only in that case; immediate errors are returned.
-- 'PartitionOffsetStored' is not supported by native seek (use 'rewindConsumer').
seekPartitions :: (MonadIO m) => KafkaConsumer -> [TopicPartition] -> Timeout -> m (Maybe KafkaError)
seekPartitions _ ps _ | Just failure <- validateTopicPartitions [(tpTopicName p, tpPartition p) | p <- ps] = pure (Just failure)
seekPartitions consumer@(KafkaConsumer (Kafka k) _) ps (Timeout timeout) = liftIO $ withForeignPtr k $ \_ -> mask_ $ bracket (toNativeTopicPartitionList ps) finalizeForeignPtr $ \tps -> do
  err <- bracket (rdKafkaSeekPartitions k tps timeout) rdKafkaErrorDestroy rdKafkaErrorCode
  case rdKafkaErrorToEither err of
    Left failure -> pure (Just failure)
    Right () -> do
      errors <- nativeTopicPartitionErrors tps
      let initiated = Set.fromList [(tpTopicName partition, tpPartition partition) | (partition, code) <- zip ps errors, code == RdKafkaRespErrNoError || code == RdKafkaRespErrInProgress]
      discardPendingMessages (getKafkaConf consumer) initiated
      pure $ KafkaResponseError <$> find substantive errors
  where
    substantive code =
      code /= RdKafkaRespErrNoError
        && (timeout /= 0 || code /= RdKafkaRespErrInProgress)

-- | Retrieve committed offsets for topics+partitions.
committed :: (MonadIO m) => KafkaConsumer -> Timeout -> [(TopicName, PartitionId)] -> m (Either KafkaError [TopicPartition])
committed _ _ ps | Just failure <- validateTopicPartitions ps = pure (Left failure)
committed (KafkaConsumer (Kafka k) _) (Timeout timeout) tps = liftIO $ withForeignPtr k $ \_ -> bracket (toNativeTopicPartitionList' tps) finalizeForeignPtr $ \ntps -> do
  failure <- partitionListError ntps =<< rdKafkaCommitted k ntps timeout
  case failure of
    Nothing -> Right <$> fromNativeTopicPartitionList'' ntps
    Just err -> pure (Left err)

-- | Retrieve current positions (last consumed message offset+1) for the current running instance of the consumer.
-- If the current consumer hasn't received any messages for a given partition, 'PartitionOffsetInvalid' is returned.
position :: (MonadIO m) => KafkaConsumer -> [(TopicName, PartitionId)] -> m (Either KafkaError [TopicPartition])
position _ ps | Just failure <- validateTopicPartitions ps = pure (Left failure)
position (KafkaConsumer (Kafka k) _) tps = liftIO $ withForeignPtr k $ \_ -> bracket (toNativeTopicPartitionList' tps) finalizeForeignPtr $ \ntps -> do
  failure <- partitionListError ntps =<< rdKafkaPosition k ntps
  case failure of
    Nothing -> Right <$> fromNativeTopicPartitionList'' ntps
    Just err -> pure (Left err)

-- | Polls the provided kafka consumer for events.
--
-- Events will cause application provided callbacks to be called.
--
-- The 'Timeout' argument specifies the maximum amount of time
-- (in milliseconds) that the call will block waiting for events.
--
-- In async mode a background thread serves callbacks periodically. In sync
-- mode 'pollMessage' serves callbacks during its native poll. Calling this
-- function explicitly in sync mode copies any returned records or attributed
-- errors into a private FIFO for the next application polls; native messages
-- are released before this call returns. Empty polls add nothing to the FIFO.
--
-- In async mode application records use separate partition queues. Any message
-- returned on the callback queue is discarded and destroyed. Fatal errors stay
-- observable through the public polling APIs and 'consumerFatalError'.
pollConsumerEvents :: KafkaConsumer -> Maybe Timeout -> IO ()
pollConsumerEvents k timeout =
  void . withCallbackPollEnabled k $ pollConsumerEvents' k timeout

-- | Closes the consumer gracefully (@rd_kafka_consumer_close@): stops the
-- background callback-poll loop, releases the message queue, and leaves the
-- consumer group, committing offsets if auto-commit is enabled and serving the
-- final rebalance callback. Against an unresponsive broker this can block; use
-- 'destroyConsumer' to tear the client down within a bound.
--
-- Closing does not free the librdkafka client; its finalizer does that when
-- the consumer is garbage collected, or 'destroyConsumer' does it now. Closing
-- again, or after 'destroyConsumer', does nothing and returns 'Nothing'.
--
-- See 'newConsumer'
closeConsumer :: (MonadIO m) => KafkaConsumer -> m (Maybe KafkaError)
closeConsumer (KafkaConsumer (Kafka k) (KafkaConf _ qr statusVar lifecycle _ pending)) = liftIO $
  modifyMVar lifecycle $ \case
    ClientOpen -> do
      -- because closing the consumer will raise callbacks,
      -- prevent the async loop from doing it at the same time
      res <- modifyMVar statusVar $ \_ -> do
        -- librdkafka says:
        --   Prior to destroying the client instance, loose your reference to the
        --   background queue by calling rd_kafka_queue_destroy()
        atomicModifyIORef' pending (const (emptyPendingMessages, ()))
        releaseMessageQueue qr
        res <- kafkaErrorToMaybe . KafkaResponseError <$> rdKafkaConsumerClose k
        pure (CallbackPollDisabled, res)
      pure (ClientClosed, res)
    state -> pure (state, Nothing)

-- | Destroys the consumer's librdkafka client now, synchronously and exactly
-- once: stops the background callback-poll loop (waiting for a poll in
-- progress), releases the message queue, and runs the client's finalizer,
-- @rd_kafka_destroy_flags@ with @RD_KAFKA_DESTROY_F_NO_CONSUMER_CLOSE@. When it
-- returns, librdkafka's threads have stopped and its broker connections are
-- closed, without waiting for a garbage collection.
--
-- It does not leave the consumer group gracefully or commit offsets; call
-- 'closeConsumer' first for that. Destroying again, or closing after
-- destroying, does nothing; concurrent calls wait until the destruction has
-- finished. If a destroy is interrupted by an asynchronous exception, a later
-- call completes it.
--
-- Every other operation on the consumer is invalid once this has been called,
-- including 'consumerClosed'. Serialize normal operations with teardown, and do
-- not call this from a librdkafka callback.
destroyConsumer :: (MonadIO m) => KafkaConsumer -> m ()
destroyConsumer (KafkaConsumer (Kafka k) (KafkaConf _ qr statusVar lifecycle _ pending)) = liftIO $
  modifyMVarMasked_ lifecycle $ \case
    ClientDestroyed -> pure ClientDestroyed
    _ -> do
      -- Each step is safe to repeat, so a destroy interrupted before it
      -- published ClientDestroyed can be completed by the next call.
      modifyMVarMasked_ statusVar (const (pure CallbackPollDisabled))
      atomicModifyIORef' pending (const (emptyPendingMessages, ()))
      releaseMessageQueue qr
      finalizeForeignPtr k
      pure ClientDestroyed

-- | Release the async message queue, if any, exactly once.
releaseMessageQueue :: IORef (Maybe RdKafkaQueueTPtr) -> IO ()
releaseMessageQueue qr = atomicModifyIORef' qr (Nothing,) >>= mapM_ rdKafkaQueueDestroy

-- | Rewind the current assignment to the committed next offsets, or beginning
-- when no offset is committed. Assignment, lookup and seek failures propagate.
-- Lookup and seek are separate operations: callers must serialize assignment
-- changes when a stable assignment is required. Successful seek members are
-- not rolled back if another member fails.
--
-- Follows @mori://confluentinc/librdkafka@, project-relative
-- @librdkafka/examples/transactions.c@ (source artifact URI pending).
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
  (forM_ cbs (\(Callback setCb) -> setCb conf) >> pure conf)
    `onException` releaseUnattachedCallbacks (kcfgCallbackOwner conf)

-- | Subscribes to a given list of topics.
--
-- Wildcard (regex) topics are supported by the /librdkafka/ assignor:
-- any topic name in the topics list that is prefixed with @^@ will
-- be regex-matched to the full list of topics in the cluster and matching
-- topics will be added to the subscription list.
subscribe :: KafkaConsumer -> Set TopicName -> IO (Maybe KafkaError)
subscribe (KafkaConsumer (Kafka k) _) ts = withForeignPtr k $ \_ -> bracket (newRdKafkaTopicPartitionListT (length ts)) finalizeForeignPtr $ \pl -> do
  mapM_ (\(TopicName t) -> rdKafkaTopicPartitionListAdd pl (Text.unpack t) (-1)) (Set.toList ts)
  res <- KafkaResponseError <$> rdKafkaSubscribe k pl
  return $ kafkaErrorToMaybe res

-- The caller's input list is borrowed by native operations. Release its
-- private native references before the client can be finalized or destroyed.
withPartitionList :: KafkaConsumer -> IO RdKafkaTopicPartitionListTPtr -> (RdKafkaTopicPartitionListTPtr -> IO a) -> IO a
withPartitionList consumer acquire action = withForeignPtr (getRdKafka consumer) $ \_ ->
  bracket acquire finalizeForeignPtr action

commitOffsets :: OffsetCommit -> KafkaConsumer -> RdKafkaTopicPartitionListTPtr -> IO (Maybe KafkaError)
commitOffsets o (KafkaConsumer (Kafka k) _) pl =
  kafkaErrorToMaybe . KafkaResponseError <$> rdKafkaCommit k pl (offsetCommitToBool o)

commitOffsetsStore :: KafkaConsumer -> RdKafkaTopicPartitionListTPtr -> IO (Maybe KafkaError)
commitOffsetsStore (KafkaConsumer (Kafka k) _) pl =
  partitionListError pl =<< rdKafkaOffsetsStore k pl

-- Preserve an overall error first, then the first failing member in input order.
-- Successful members of a mixed native request are not rolled back.
partitionListError :: RdKafkaTopicPartitionListTPtr -> RdKafkaRespErrT -> IO (Maybe KafkaError)
partitionListError partitions overall = case rdKafkaErrorToEither overall of
  Left failure -> pure (Just failure)
  Right () -> fmap KafkaResponseError . find (/= RdKafkaRespErrNoError) <$> nativeTopicPartitionErrors partitions

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
-- releases whatever native message that poll hands back. Sync-mode results
-- are copied into the application FIFO before release.
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
pollConsumerEvents' k timeout = withForeignPtr (getRdKafka k) $ \_ -> do
  let (Timeout tm) = fromMaybe (Timeout 0) timeout
  queue <- getRdMsgQueue k
  withPolledMessage (rdKafkaConsumerPoll (getRdKafka k) tm) $ \message ->
    case queue of
      Just _ -> pure ()
      Nothing -> withForeignPtr message $ \raw -> when (raw /= nullPtr) $ do
        copied <- copyMessagePtrAttributed message
        case copied of
          Left (ConsumerMessageError (KafkaResponseError RdKafkaRespErrFatal) _ _ _) -> pure ()
          _ -> atomicModifyIORef' (kcfgPendingMessages (getKafkaConf k)) $ \buffer ->
            (buffer {bufferedMessages = bufferedMessages buffer |> packPendingMessage copied}, ())

-- A cached result must not bypass ready rebalance/commit callbacks or stop
-- updating native poll liveness while the application drains copied records.
servePendingCallbacks :: KafkaConsumer -> IO ()
servePendingCallbacks consumer =
  getRdMsgQueue consumer >>= \case
    Just _ -> pure ()
    Nothing -> do
      buffer <- readIORef (kcfgPendingMessages (getKafkaConf consumer))
      case Seq.viewl (bufferedMessages buffer) of
        EmptyL -> pure ()
        _ -> pollConsumerEvents consumer (Just (Timeout 0))

-- Callback polling copies messages before releasing their native owners. The
-- buffer stores no client references, so it cannot form a callback/client cycle.
packPendingMessage :: Either ConsumerMessageError (ConsumerRecord (Maybe BS.ByteString) (Maybe BS.ByteString)) -> PendingMessage
packPendingMessage = \case
  Left (ConsumerMessageError failure topic partition offset) -> PendingMessageError failure topic partition (unOffset <$> offset)
  Right (ConsumerRecord topic partition (Offset offset) timestamp headers key value) ->
    PendingMessageRecord topic partition offset (packTimestamp timestamp) headers key value
  where
    packTimestamp NoTimestamp = PendingNoTimestamp
    packTimestamp (CreateTime time) = PendingCreateTime time
    packTimestamp (LogAppendTime time) = PendingLogAppendTime time

nextPendingMessage :: KafkaConsumer -> IO (Maybe (Either ConsumerMessageError (ConsumerRecord (Maybe BS.ByteString) (Maybe BS.ByteString))))
nextPendingMessage consumer = atomicModifyIORef' (kcfgPendingMessages (getKafkaConf consumer)) $ \buffer ->
  let ready = maybe True (`Set.notMember` pendingPausedPartitions buffer) . pendingMessagePartition
      (held, available) = Seq.breakl ready (bufferedMessages buffer)
   in case Seq.viewl available of
        EmptyL -> (buffer, Nothing)
        value :< rest -> (buffer {bufferedMessages = held Seq.>< rest}, Just (unpack value))
  where
    unpack (PendingMessageError failure topic partition offset) = Left (ConsumerMessageError failure topic partition (Offset <$> offset))
    unpack (PendingMessageRecord topic partition offset timestamp headers key value) =
      Right (ConsumerRecord topic partition (Offset offset) (unpackTimestamp timestamp) headers key value)
    unpackTimestamp PendingNoTimestamp = NoTimestamp
    unpackTimestamp (PendingCreateTime time) = CreateTime time
    unpackTimestamp (PendingLogAppendTime time) = LogAppendTime time

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
