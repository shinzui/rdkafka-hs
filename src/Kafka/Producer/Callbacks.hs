{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TypeApplications #-}

module Kafka.Producer.Callbacks
  ( deliveryCallback,
    module X,
  )
where

import Control.Concurrent (forkIOWithUnmask)
import Control.Exception (SomeException, bracket, catch, mask, throwIO, try)
import Control.Monad (void)
import Data.Either (fromRight)
import Foreign.C.Error (getErrno)
import Foreign.Ptr (Ptr, nullPtr)
import Foreign.StablePtr (castPtrToStablePtr, deRefStablePtr, freeStablePtr)
import Foreign.Storable (Storable (peek))
import Kafka.Callbacks as X
import Kafka.Consumer.Types (Offset (..))
import Kafka.Internal.RdKafka (RdKafkaMessageT (..), RdKafkaRespErrT (..), rdKafkaConfSetDrMsgCb)
import Kafka.Internal.Setup (Callback (..), KafkaConf (kcfgCallbackOwner), getRdKafkaConf)
import Kafka.Internal.Shared (kafkaRespErr, readHeaders, readKey, readPayload, readTopic)
import Kafka.Producer.Types (DeliveryReport (..), ProducePartition (..), ProducerRecord (..))
import Kafka.Types (KafkaError (..), TopicName (..))

-- | Sets the callback for delivery reports.
--
--   /Note: A callback should not be a long-running process as it blocks
--   librdkafka from continuing on the thread that handles the delivery
--   callbacks. For callbacks to individual messsages see
--   'Kafka.Producer.produceMessage\''./
--
-- Exceptions from a global callback are reported through the runtime's
-- uncaught-exception handler on a separate Haskell thread. They do not unwind
-- through librdkafka or prevent the per-message callback from running. A
-- per-message callback runs unmasked on its own thread and uses that same
-- runtime exception policy.
deliveryCallback :: (DeliveryReport -> IO ()) -> Callback
deliveryCallback callback = Callback $ \kc -> rdKafkaConfSetDrMsgCb (kcfgCallbackOwner kc) (getRdKafkaConf kc) realCb
  where
    realCb :: t -> Ptr RdKafkaMessageT -> IO ()
    realCb _ mptr = containCallbackException $ mask $ \restore ->
      if mptr == nullPtr
        then getErrno >>= (restore . callback . NoMessageError . kafkaRespErr)
        else do
          s <- peek mptr
          let cbPtr = opaque'RdKafkaMessageT s
              deliver messageCallback = do
                prodRec <- mkProdRec mptr
                let rep =
                      if err'RdKafkaMessageT s /= RdKafkaRespErrNoError
                        then mkErrorReport s prodRec
                        else mkSuccessReport s prodRec
                outcome <- try @SomeException (restore (callback rep))
                -- Forking transfers the closure to a Haskell thread before
                -- the StablePtr root is released, even if the global callback
                -- threw. A long per-message callback never blocks native poll.
                mapM_ (\msgCb -> void $ forkIOWithUnmask (\unmask -> unmask (msgCb rep))) messageCallback
                either throwIO pure outcome
          if cbPtr == nullPtr
            then deliver Nothing
            else bracket (pure $ castPtrToStablePtr cbPtr) freeStablePtr $ \stablePtr -> do
              msgCb <- deRefStablePtr @(DeliveryReport -> IO ()) stablePtr
              deliver (Just msgCb)

-- A Haskell exception cannot cross a foreign callback frame. Throwing on a
-- fresh Haskell thread retains the ordinary uncaught-exception reporting
-- policy without retaining the native message or its StablePtr.
containCallbackException :: IO () -> IO ()
containCallbackException action =
  action `catch` \(exception :: SomeException) ->
    void $ forkIOWithUnmask (\unmask -> unmask (throwIO exception))

mkErrorReport :: RdKafkaMessageT -> ProducerRecord -> DeliveryReport
mkErrorReport msg prodRec = DeliveryFailure prodRec (KafkaResponseError (err'RdKafkaMessageT msg))

mkSuccessReport :: RdKafkaMessageT -> ProducerRecord -> DeliveryReport
mkSuccessReport msg prodRec = DeliverySuccess prodRec (Offset $ offset'RdKafkaMessageT msg)

mkProdRec :: Ptr RdKafkaMessageT -> IO ProducerRecord
mkProdRec pmsg = do
  msg <- peek pmsg
  topic <- readTopic msg
  key <- readKey msg
  payload <- readPayload msg
  flip fmap (fromRight mempty <$> readHeaders pmsg) $ \headers ->
    ProducerRecord
      { prTopic = TopicName topic,
        prPartition = SpecifiedPartition (partition'RdKafkaMessageT msg),
        prKey = key,
        prValue = payload,
        prHeaders = headers
      }
