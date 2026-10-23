{-# LANGUAGE LambdaCase #-}

module Kafka.Internal.Shared
  ( pollEvents,
    word8PtrToBS,
    kafkaRespErr,
    throwOnError,
    hasError,
    rdKafkaErrorToEither,
    kafkaErrorToEither,
    kafkaErrorToMaybe,
    maybeToLeft,
    readHeaders,
    readPayload,
    readTopic,
    readKey,
    readTimestamp,
    readBS,
    setSaslCredentials',
  )
where

import Control.Exception (throw)
import Control.Monad (void)
import Data.ByteString qualified as BS
import Data.ByteString.Internal qualified as BSI
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Word (Word8)
import Foreign.C.Error (Errno (..))
import Foreign.ForeignPtr (newForeignPtr_)
import Foreign.Marshal.Alloc (alloca)
import Foreign.Marshal.Utils (copyBytes)
import Foreign.Ptr (Ptr, nullPtr)
import Foreign.Storable (Storable (peek))
import Kafka.Consumer.Types (Timestamp (..))
import Kafka.Internal.RdKafka (RdKafkaMessageT (..), RdKafkaMessageTPtr, RdKafkaRespErrT (..), RdKafkaTimestampTypeT (..), Word8Ptr, rdKafkaErrno2err, rdKafkaHeaderGetAll, rdKafkaMessageHeaders, rdKafkaMessageTimestamp, rdKafkaPoll, rdKafkaSaslSetCredentials, rdKafkaTopicName)
import Kafka.Internal.Setup (HasKafka (..), Kafka (..))
import Kafka.Types (Headers, KafkaError (..), Millis (..), Timeout (..), headersFromList)

-- | Replace a client's SASL username and password
-- (@rd_kafka_sasl_set_credentials@).
setSaslCredentials' :: (HasKafka a) => a -> Text -> Text -> IO (Maybe KafkaError)
setSaslCredentials' a username password =
  let Kafka k = getKafka a
   in kafkaErrorToMaybe . KafkaResponseError
        <$> rdKafkaSaslSetCredentials k (Text.unpack username) (Text.unpack password)

pollEvents :: (HasKafka a) => a -> Maybe Timeout -> IO ()
pollEvents a tm =
  let timeout = maybe 0 unTimeout tm
      Kafka k = getKafka a
   in void (rdKafkaPoll k timeout)

word8PtrToBS :: Int -> Word8Ptr -> IO BS.ByteString
word8PtrToBS len ptr = BSI.create len $ \bsptr ->
  copyBytes bsptr ptr len

kafkaRespErr :: Errno -> KafkaError
kafkaRespErr (Errno num) = KafkaResponseError $ rdKafkaErrno2err (fromIntegral num)
{-# INLINE kafkaRespErr #-}

throwOnError :: IO (Maybe Text) -> IO ()
throwOnError action = do
  m <- action
  case m of
    Just e -> throw $ KafkaError e
    Nothing -> return ()

hasError :: KafkaError -> Bool
hasError err = case err of
  KafkaResponseError RdKafkaRespErrNoError -> False
  _ -> True
{-# INLINE hasError #-}

rdKafkaErrorToEither :: RdKafkaRespErrT -> Either KafkaError ()
rdKafkaErrorToEither err = case err of
  RdKafkaRespErrNoError -> Right ()
  _ -> Left (KafkaResponseError err)
{-# INLINE rdKafkaErrorToEither #-}

kafkaErrorToEither :: KafkaError -> Either KafkaError ()
kafkaErrorToEither err = case err of
  KafkaResponseError RdKafkaRespErrNoError -> Right ()
  _ -> Left err
{-# INLINE kafkaErrorToEither #-}

kafkaErrorToMaybe :: KafkaError -> Maybe KafkaError
kafkaErrorToMaybe err = case err of
  KafkaResponseError RdKafkaRespErrNoError -> Nothing
  _ -> Just err
{-# INLINE kafkaErrorToMaybe #-}

maybeToLeft :: Maybe a -> Either a ()
maybeToLeft = maybe (Right ()) Left
{-# INLINE maybeToLeft #-}

readPayload :: RdKafkaMessageT -> IO (Maybe BS.ByteString)
readPayload = readBS len'RdKafkaMessageT payload'RdKafkaMessageT

readTopic :: RdKafkaMessageT -> IO Text
readTopic msg = newForeignPtr_ (topic'RdKafkaMessageT msg) >>= (fmap Text.pack . rdKafkaTopicName)

readKey :: RdKafkaMessageT -> IO (Maybe BSI.ByteString)
readKey = readBS keyLen'RdKafkaMessageT key'RdKafkaMessageT

readTimestamp :: RdKafkaMessageTPtr -> IO Timestamp
readTimestamp msg =
  alloca $ \p -> do
    typeP <- newForeignPtr_ p
    ts <- fromIntegral <$> rdKafkaMessageTimestamp msg typeP
    tsType <- peek p
    return $ case tsType of
      RdKafkaTimestampCreateTime -> CreateTime (Millis ts)
      RdKafkaTimestampLogAppendTime -> LogAppendTime (Millis ts)
      RdKafkaTimestampNotAvailable -> NoTimestamp

readHeaders :: Ptr RdKafkaMessageT -> IO (Either RdKafkaRespErrT Headers)
readHeaders msg = do
  (err, headersPtr) <- rdKafkaMessageHeaders msg
  case err of
    RdKafkaRespErrNoent -> return $ Right mempty
    RdKafkaRespErrNoError -> fmap headersFromList <$> extractHeaders headersPtr
    e -> return . Left $ e
  where
    extractHeaders ptHeaders =
      alloca $ \nptr ->
        alloca $ \vptr ->
          alloca $ \szptr ->
            let go acc idx =
                  rdKafkaHeaderGetAll ptHeaders idx nptr vptr szptr >>= \case
                    RdKafkaRespErrNoent -> return . Right $ reverse acc
                    RdKafkaRespErrNoError -> do
                      cstr <- peek nptr
                      wptr <- peek vptr
                      csize <- peek szptr
                      hn <- BS.packCString cstr
                      hv <- word8PtrToBS (fromIntegral csize) wptr
                      go ((hn, hv) : acc) (idx + 1)
                    _ -> error "Unexpected error code while extracting headers"
             in go [] 0

readBS :: (t -> Int) -> (t -> Ptr Word8) -> t -> IO (Maybe BS.ByteString)
readBS flen fdata s =
  if fdata s == nullPtr
    then return Nothing
    else Just <$> word8PtrToBS (flen s) (fdata s)
