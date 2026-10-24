{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TupleSections #-}

-----------------------------------------------------------------------------

-----------------------------------------------------------------------------

-- |
-- Module to produce messages to Kafka topics.
--
-- Here's an example of code to produce messages to a topic:
--
-- @
-- import Control.Exception (bracket)
-- import Control.Monad (forM_)
-- import Data.ByteString (ByteString)
-- import Kafka.Producer
--
-- -- Global producer properties
-- producerProps :: 'ProducerProperties'
-- producerProps = 'brokersList' ["localhost:9092"]
--              <> 'logLevel' 'KafkaLogDebug'
--
-- -- Topic to send messages to
-- targetTopic :: 'TopicName'
-- targetTopic = 'TopicName' "kafka-client-example-topic"
--
-- -- Run an example
-- runProducerExample :: IO ()
-- runProducerExample =
--     bracket mkProducer clProducer runHandler >>= print
--     where
--       mkProducer = 'newProducer' producerProps
--       clProducer (Left _)     = pure ()
--       clProducer (Right prod) = 'closeProducer' prod
--       runHandler (Left err)   = pure $ Left err
--       runHandler (Right prod) = sendMessages prod
--
-- -- Example sending 2 messages and printing the response from Kafka
-- sendMessages :: 'KafkaProducer' -> IO (Either 'KafkaError' ())
-- sendMessages prod = do
--   err1 <- 'produceMessage' prod (mkMessage Nothing (Just "test from producer") )
--   forM_ err1 print
--
--   err2 <- 'produceMessage' prod (mkMessage (Just "key") (Just "test from producer (with key)"))
--   forM_ err2 print
--
--   pure $ Right ()
--
-- mkMessage :: Maybe ByteString -> Maybe ByteString -> 'ProducerRecord'
-- mkMessage k v = 'ProducerRecord'
--                   { 'prTopic' = targetTopic
--                   , 'prPartition' = 'UnassignedPartition'
--                   , 'prKey' = k
--                   , 'prValue' = v
--                   }
-- @
module Kafka.Producer
  ( KafkaProducer,
    module X,
    runProducer,
    newProducer,
    produceMessage,
    produceMessage',
    flushProducer,
    flushProducerTimeout,
    purgeProducer,
    setSaslCredentials,
    closeProducer,
    RdKafkaRespErrT (..),
  )
where

import Kafka.Internal.CallbackOwner (releaseUnattachedCallbacks)

import Control.Exception (bracket, mask, mask_, onException)
import Control.Monad (forM_)
import Control.Monad.IO.Class (MonadIO (liftIO))
import Data.Bits ((.|.))
import Data.ByteString qualified as BS
import Data.ByteString.Internal qualified as BSI
import Data.Text (Text)
import Data.Text qualified as Text
import Foreign.C.String (withCString)
import Foreign.C.Types (CInt)
import Foreign.ForeignPtr (withForeignPtr)
import Foreign.Marshal.Alloc (allocaBytes)
import Foreign.Marshal.Utils (withMany)
import Foreign.Ptr (Ptr, nullPtr, plusPtr)
import Foreign.StablePtr (castStablePtrToPtr, freeStablePtr, newStablePtr)
import Kafka.Internal.RdKafka (RdKafkaRespErrT (..), RdKafkaTypeT (..), RdKafkaVuT (..), newRdKafkaT, rdKafkaErrorCode, rdKafkaErrorDestroy, rdKafkaFlush, rdKafkaMessageProduceVa, rdKafkaPurge, rdKafkaPurgeFInflight, rdKafkaPurgeFNonBlocking, rdKafkaPurgeFQueue, rdKafkaSetLogLevel)
import Kafka.Internal.Setup (Callback (..), Kafka (..), KafkaConf (..), KafkaProps (..), TopicProps (..), getRdKafka, kafkaConf, setAllKafkaConfValues, topicConf)
import Kafka.Internal.Shared (kafkaErrorToMaybe, pollEvents, setSaslCredentials')
import Kafka.Producer.Convert (copyMsgFlags, handleProduceErrT, producePartitionCInt)
import Kafka.Producer.Types (KafkaProducer (..))

import Kafka.Producer.ProducerProperties as X
import Kafka.Producer.Types as X hiding (KafkaProducer)
import Kafka.Types as X

-- | Runs Kafka Producer.
-- The callback provided is expected to call 'produceMessage'
-- to send messages to Kafka.
{-# DEPRECATED runProducer "Use 'newProducer'/'closeProducer' instead" #-}
runProducer ::
  ProducerProperties ->
  (KafkaProducer -> IO (Either KafkaError a)) ->
  IO (Either KafkaError a)
runProducer props f =
  bracket mkProducer clProducer runHandler
  where
    mkProducer = newProducer props

    clProducer (Left _) = return ()
    clProducer (Right prod) = closeProducer prod

    runHandler (Left err) = return $ Left err
    runHandler (Right prod) = f prod

-- | Creates a new kafka producer
-- A newly created producer must be closed with 'closeProducer' function.
-- Explicit topic properties override matching global properties; unrelated
-- global properties are preserved. Invalid topic properties retain the
-- existing configuration-exception convention.
newProducer :: (MonadIO m) => ProducerProperties -> m (Either KafkaError KafkaProducer)
newProducer pps = liftIO $ mask $ \restore -> do
  kc@(KafkaConf kc' _ _ _ _) <- kafkaConf (KafkaProps $ (ppKafkaProps pps))
  flip onException (releaseUnattachedCallbacks (kcfgCallbackOwner kc)) $ restore $ do
    tc <- topicConf (TopicProps $ (ppTopicProps pps))
    -- librdkafka accepts topic properties on the global configuration. Merge
    -- only explicit keys: replacing default_topic_conf would discard topic
    -- properties already supplied through the global map.
    setAllKafkaConfValues kc (KafkaProps (ppTopicProps pps))

    -- add default delivery report callback
    let Callback setDeliveryCallback = deliveryCallback (const mempty)
    setDeliveryCallback kc

    -- set callbacks
    forM_ (ppCallbacks pps) (\(Callback setCb) -> setCb kc)

    mbKafka <- newRdKafkaT (kcfgCallbackOwner kc) RdKafkaProducer kc'
    case mbKafka of
      Left err -> return . Left $ KafkaError err
      Right kafka -> do
        forM_ (ppLogLevel pps) (rdKafkaSetLogLevel kafka . fromEnum)
        let prod = KafkaProducer (Kafka kafka) kc tc
        return (Right prod)

-- | Sends a single message.
-- Since librdkafka is backed by a queue, this function can return before messages are sent. See
-- 'flushProducer' to wait for queue to empty.
produceMessage ::
  (MonadIO m) =>
  KafkaProducer ->
  ProducerRecord ->
  m (Maybe KafkaError)
produceMessage kp m = produceMessage' kp m (pure . mempty) >>= adjustRes
  where
    adjustRes = \case
      Right () -> pure Nothing
      Left (ImmediateError err) -> pure (Just err)

-- | Sends a single message with a registered callback.
--
--   The callback can be a long running process, as it is forked by the thread
--   that handles the delivery reports.
--
-- Absent and empty keys/values remain distinct. Topic and header names
-- containing NUL, and partition numbers outside the native C integer range,
-- return an immediate 'KafkaBadSpecification' before enqueueing.
produceMessage' ::
  (MonadIO m) =>
  KafkaProducer ->
  ProducerRecord ->
  (DeliveryReport -> IO ()) ->
  m (Either ImmediateError ())
produceMessage' kp@(KafkaProducer (Kafka k) _ _) msg cb =
  liftIO $ case recordError msg of
    Just err -> pure (Left (ImmediateError err))
    Nothing -> fireCallbacks >> produceIt
  where
    fireCallbacks =
      pollEvents kp . Just . Timeout $ 0

    produceIt =
      withBS (prValue msg) $ \payloadPtr payloadLength ->
        withBS (prKey msg) $ \keyPtr keyLength ->
          withHeaders (prHeaders msg) $ \hdrs ->
            withCString (Text.unpack . unTopicName . prTopic $ msg) $ \topicName -> mask_ $ do
              callbackPtr <- newStablePtr cb
              let opts =
                    [ Topic'RdKafkaVu topicName,
                      Partition'RdKafkaVu . producePartitionCInt . prPartition $ msg,
                      MsgFlags'RdKafkaVu (fromIntegral copyMsgFlags),
                      Value'RdKafkaVu payloadPtr (fromIntegral payloadLength),
                      Key'RdKafkaVu keyPtr (fromIntegral keyLength),
                      Opaque'RdKafkaVu (castStablePtrToPtr callbackPtr)
                    ]

              -- Once enqueue succeeds another poller can already have served
              -- the report and freed this root. Only an immediate rejection
              -- leaves ownership here; no cleanup may cover the success path.
              nativeError <- rdKafkaMessageProduceVa k (hdrs ++ opts) `onException` freeStablePtr callbackPtr
              if nativeError == nullPtr
                then pure (Right ())
                else bracket (pure nativeError) rdKafkaErrorDestroy $ \native -> do
                  -- A non-null error object means rejection. Release before
                  -- decoding it, so even a conversion exception cannot retain
                  -- the callback. Only NULL transfers ownership to delivery.
                  freeStablePtr callbackPtr
                  code <- rdKafkaErrorCode native
                  res <- handleProduceErrT code
                  pure $ Left (ImmediateError (maybe KafkaInvalidReturnValue id res))

-- | Closes the producer.
-- Will wait until the outbound queue is drained before returning the control.
closeProducer :: (MonadIO m) => KafkaProducer -> m ()
closeProducer = flushProducer

-- | Drains the outbound queue for a producer, blocking until every queued
-- message has been delivered or has failed and its delivery report has been
-- served.
--
-- This function is also called automatically when the producer is closed
-- with 'closeProducer' to ensure that all queued messages make it to Kafka.
-- It never gives up; use 'flushProducerTimeout' to bound the wait.
flushProducer :: (MonadIO m) => KafkaProducer -> m ()
flushProducer kp = liftIO go
  where
    go =
      flushProducerTimeout kp (Timeout 1000) >>= \case
        Just (KafkaResponseError RdKafkaRespErrTimedOut) -> go
        _ -> pure ()

-- | Waits up to the timeout for every queued message to be delivered or to
-- fail (@rd_kafka_flush@), serving delivery callbacks meanwhile.
--
-- Returns 'Nothing' once the outbound queue is empty, and
-- @'Just' ('KafkaResponseError' 'RdKafkaRespErrTimedOut')@ when messages are
-- still outstanding after the timeout.
flushProducerTimeout :: (MonadIO m) => KafkaProducer -> Timeout -> m (Maybe KafkaError)
flushProducerTimeout kp (Timeout ms) =
  liftIO $ kafkaErrorToMaybe . KafkaResponseError <$> rdKafkaFlush (getRdKafka kp) ms

-- | Purges messages from the producer (@rd_kafka_purge@): those still queued
-- with 'PurgeQueue', those awaiting a broker response with 'PurgeInFlight'.
-- An empty list purges nothing.
--
-- Purged messages are not lost silently: each is reported to the delivery
-- callback as a 'DeliveryFailure' with
-- @'KafkaResponseError' 'RdKafkaRespErrPurgeQueue'@ or
-- @'KafkaResponseError' 'RdKafkaRespErrPurgeInflight'@, the next time
-- callbacks are served (for example by 'flushProducer'). A purge does not
-- abort an in-flight request; its response is ignored. Unless
-- 'PurgeNonBlocking' is given, the call waits for librdkafka's background
-- threads to acknowledge the purge.
purgeProducer :: (MonadIO m) => KafkaProducer -> [PurgeFlag] -> m (Maybe KafkaError)
purgeProducer kp flags =
  liftIO $ kafkaErrorToMaybe . KafkaResponseError <$> rdKafkaPurge (getRdKafka kp) (foldr ((.|.) . purgeFlagBits) 0 flags)
  where
    purgeFlagBits = \case
      PurgeQueue -> rdKafkaPurgeFQueue
      PurgeInFlight -> rdKafkaPurgeFInflight
      PurgeNonBlocking -> rdKafkaPurgeFNonBlocking

-- | Replaces the SASL username and password the producer uses for future
-- authentications (@rd_kafka_sasl_set_credentials@); existing connections
-- are not re-authenticated. librdkafka stores the credentials whether or not
-- SASL is configured.
setSaslCredentials ::
  (MonadIO m) =>
  KafkaProducer ->
  -- | username
  Text ->
  -- | password
  Text ->
  m (Maybe KafkaError)
setSaslCredentials kp username password = liftIO $ setSaslCredentials' kp username password

------------------------------------------------------------------------------------

recordError :: ProducerRecord -> Maybe KafkaError
recordError record
  | Text.any (== '\0') (unTopicName (prTopic record)) = invalid "topic name contains NUL"
  | any (BS.elem 0 . fst) (headersToList (prHeaders record)) = invalid "header name contains NUL"
  | SpecifiedPartition partition <- prPartition record,
    toInteger partition < toInteger (minBound :: CInt) || toInteger partition > toInteger (maxBound :: CInt) =
      invalid "partition id is outside the C int range"
  | otherwise = Nothing
  where
    invalid = Just . KafkaBadSpecification

withHeaders :: Headers -> ([RdKafkaVuT] -> IO a) -> IO a
withHeaders hds = withMany allocHeader (headersToList hds)
  where
    allocHeader (nm, val) f =
      BS.useAsCString nm $ \cnm ->
        withBS (Just val) $ \vp vl ->
          f $ Header'RdKafkaVu cnm vp (fromIntegral vl)

withBS :: Maybe BS.ByteString -> (Ptr a -> Int -> IO b) -> IO b
withBS Nothing f = f nullPtr 0
-- The empty ByteString can have a null backing pointer. Native Kafka uses
-- null to mean an absent key/value, even with length zero, so keep Just empty
-- distinct from Nothing with a non-null temporary pointer.
withBS (Just bs) f | BS.null bs = allocaBytes 1 $ \p -> f p 0
withBS (Just bs) f =
  let (d, o, l) = BSI.toForeignPtr bs
   in withForeignPtr d $ \p -> f (p `plusPtr` o) l
