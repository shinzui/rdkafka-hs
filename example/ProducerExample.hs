{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

module ProducerExample
  ( producerProps,
    targetTopic,
    mkMessage,
    runProducerExample,
    runProducerExampleWith,
    sendMessages,
    sendMessageSync,
  )
where

import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import Control.Exception (bracket)
import Control.Monad (forM_)
import Control.Monad.IO.Class (MonadIO (..))
import Data.ByteString (ByteString)
import Data.ByteString.Char8 (pack)
import Kafka.Consumer (Offset)
import Kafka.Producer

-- Global producer properties
producerProps :: ProducerProperties
producerProps =
  brokersList ["localhost:9092"]
    <> sendTimeout (Timeout 10000)
    <> setCallback (deliveryCallback print)
    <> logLevel KafkaLogDebug

-- Topic to send messages to
targetTopic :: TopicName
targetTopic = "kafka-client-example-topic"

mkMessage :: Maybe ByteString -> Maybe ByteString -> ProducerRecord
mkMessage k v =
  ProducerRecord
    { prTopic = targetTopic,
      prPartition = UnassignedPartition,
      prKey = k,
      prValue = v,
      prHeaders = mempty
    }

-- Run an example
runProducerExample :: IO ()
runProducerExample = runProducerExampleWith producerProps targetTopic

-- | Use an isolated broker/topic while retaining the interactive example.
runProducerExampleWith :: ProducerProperties -> TopicName -> IO ()
runProducerExampleWith props topic =
  bracket mkProducer clProducer runHandler >>= print
  where
    mkProducer = newProducer props
    clProducer (Left _) = return ()
    clProducer (Right prod) = closeProducer prod
    runHandler (Left err) = return $ Left err
    runHandler (Right prod) = sendMessagesTo topic prod

sendMessages :: KafkaProducer -> IO (Either KafkaError ())
sendMessages = sendMessagesTo targetTopic

sendMessagesTo :: TopicName -> KafkaProducer -> IO (Either KafkaError ())
sendMessagesTo topic prod = do
  putStrLn "Producer is ready, send your messages!"
  msg1 <- getLine

  err1 <- produceMessage prod (mkMessageFor (Just "zero") (Just $ pack msg1))
  forM_ err1 print

  putStrLn "One more time!"
  msg2 <- getLine

  err2 <- produceMessage prod (mkMessageFor (Just "key") (Just $ pack msg2))
  forM_ err2 print

  putStrLn "And the last one..."
  msg3 <- getLine
  err3 <- produceMessage prod (mkMessageFor (Just "key3") (Just $ pack msg3))
  forM_ err3 print

  err4 <- produceMessage prod ((mkMessageFor (Just "key4") (Just $ pack msg3)) {prHeaders = headersFromList [("fancy", "header")]})
  forM_ err4 print

  putStrLn "Thank you."
  return $ Right ()
  where
    mkMessageFor k v = (mkMessage k v) {prTopic = topic}

-- | An example for sending messages synchronously using the 'produceMessage''
--   function
sendMessageSync ::
  (MonadIO m) =>
  KafkaProducer ->
  ProducerRecord ->
  m (Either KafkaError Offset)
sendMessageSync producer record = liftIO $ do
  -- Create an empty MVar:
  var <- newEmptyMVar

  -- Produce the message and use the callback to put the delivery report in the
  -- MVar:
  res <- produceMessage' producer record (putMVar var)

  case res of
    Left (ImmediateError err) ->
      pure (Left err)
    Right () -> do
      -- Flush producer queue to make sure you don't get stuck waiting for the
      -- message to send:
      flushProducer producer

      -- Wait for the message's delivery report and map accordingly:
      takeMVar var
        >>= return . \case
          DeliverySuccess _ offset -> Right offset
          DeliveryFailure _ err -> Left err
          NoMessageError err -> Left err
