-----------------------------------------------------------------------------

-----------------------------------------------------------------------------

-- |
-- Module to work wih Kafkas transactional producers.
module Kafka.Transaction
  ( initTransactions,
    beginTransaction,
    commitTransaction,
    abortTransaction,
    commitOffsetMessageTransaction,
    -- , commitTransactionWithOffsets

    TxError,
    getKafkaError,
    kafkaErrorIsFatal,
    kafkaErrorIsRetriable,
    kafkaErrorTxnRequiresAbort,
  )
where

import Control.Exception (bracket, evaluate)
import Control.Monad.IO.Class (MonadIO (liftIO))
import Foreign.ForeignPtr (finalizeForeignPtr)
import Kafka.Consumer
import Kafka.Consumer.Convert (toNativeTopicPartitionList, topicPartitionFromMessageForCommit)
import Kafka.Internal.RdKafka (RdKafkaErrorTPtr, rdKafkaAbortTransaction, rdKafkaBeginTransaction, rdKafkaCommitTransaction, rdKafkaErrorCode, rdKafkaErrorDestroy, rdKafkaErrorIsFatal, rdKafkaErrorIsRetriable, rdKafkaErrorTxnRequiresAbort, rdKafkaInitTransactions, rdKafkaSendOffsetsToTransaction)
import Kafka.Internal.Setup (getRdKafka)
import Kafka.Producer
import Kafka.Producer.Convert (handleProduceErrT)

-------------------------------------------------------------------------------------
-- Tx API

data TxError = TxError
  { txErrorKafka :: !KafkaError,
    txErrorFatal :: !Bool,
    txErrorRetriable :: !Bool,
    txErrorTxnReqAbort :: !Bool
  }

-- | Initialises Kafka for transactions
initTransactions ::
  (MonadIO m) =>
  KafkaProducer ->
  Timeout ->
  m (Maybe KafkaError)
initTransactions p (Timeout to) =
  liftIO $ bracket (rdKafkaInitTransactions (getRdKafka p) to) rdKafkaErrorDestroy decodeError

-- | Begins a new transaction
beginTransaction ::
  (MonadIO m) =>
  KafkaProducer ->
  m (Maybe KafkaError)
beginTransaction p =
  liftIO $ bracket (rdKafkaBeginTransaction (getRdKafka p)) rdKafkaErrorDestroy decodeError

-- | Commits an existing transaction
-- Pre-condition: there exists an open transaction, created with beginTransaction
commitTransaction ::
  (MonadIO m) =>
  KafkaProducer ->
  Timeout ->
  m (Maybe TxError)
commitTransaction p (Timeout to) = liftIO $ bracket (rdKafkaCommitTransaction (getRdKafka p) to) rdKafkaErrorDestroy toTxError

-- | Aborts an existing transaction
-- Pre-condition: there exists an open transaction, created with beginTransaction
abortTransaction ::
  (MonadIO m) =>
  KafkaProducer ->
  Timeout ->
  m (Maybe KafkaError)
abortTransaction p (Timeout to) =
  liftIO $ bracket (rdKafkaAbortTransaction (getRdKafka p) to) rdKafkaErrorDestroy decodeError

-- | Commits the message's offset in the current transaction
--    Similar to Kafka.Consumer.commitOffsetMessage but within a transactional context
-- Pre-condition: there exists an open transaction, created with beginTransaction
commitOffsetMessageTransaction ::
  (MonadIO m) =>
  KafkaProducer ->
  KafkaConsumer ->
  ConsumerRecord k v ->
  Timeout ->
  m (Maybe TxError)
commitOffsetMessageTransaction p c m (Timeout to) = liftIO $
  bracket (toNativeTopicPartitionList [topicPartitionFromMessageForCommit m]) finalizeForeignPtr $ \tps ->
    bracket (rdKafkaSendOffsetsToTransaction (getRdKafka p) (getRdKafka c) tps to) rdKafkaErrorDestroy toTxError

-- -- | Commit offsets for all currently assigned partitions in the current transaction
-- --    Similar to Kafka.Consumer.commitAllOffsets but within a transactional context
-- -- Pre-condition: there exists an open transaction, created with beginTransaction
-- commitAllOffsetsTransaction :: MonadIO m
--                             => KafkaProducer
--                             -> KafkaConsumer
--                             -> Timeout
--                             -> m (Maybe TxError)
-- commitAllOffsetsTransaction p c (Timeout to) = liftIO $ do
--   -- TODO: this can't be right...
--   tps <- newForeignPtr_ nullPtr
--   rdKafkaSendOffsetsToTransaction (getRdKafka p) (getRdKafka c) tps to >>= toTxError

getKafkaError :: TxError -> KafkaError
getKafkaError = txErrorKafka

kafkaErrorIsFatal :: TxError -> Bool
kafkaErrorIsFatal = txErrorFatal

kafkaErrorIsRetriable :: TxError -> Bool
kafkaErrorIsRetriable = txErrorRetriable

kafkaErrorTxnRequiresAbort :: TxError -> Bool
kafkaErrorTxnRequiresAbort = txErrorTxnReqAbort

----------------------------------------------------------------------------------------------------
-- Implementation detail, used internally
decodeError :: RdKafkaErrorTPtr -> IO (Maybe KafkaError)
decodeError pointer = rdKafkaErrorCode pointer >>= handleProduceErrT

-- The caller scopes ownership around acquisition and all decoding. Strict
-- fields are evaluated before release, so no conversion escapes that scope.
toTxError :: RdKafkaErrorTPtr -> IO (Maybe TxError)
toTxError errPtr = do
  ret <- rdKafkaErrorCode errPtr >>= handleProduceErrT
  case ret of
    Nothing -> pure Nothing
    Just ke -> do
      fatal <- rdKafkaErrorIsFatal errPtr
      retriable <- rdKafkaErrorIsRetriable errPtr
      reqAbort <- rdKafkaErrorTxnRequiresAbort errPtr
      Just
        <$> evaluate
          ( TxError
              { txErrorKafka = ke,
                txErrorFatal = fatal,
                txErrorRetriable = retriable,
                txErrorTxnReqAbort = reqAbort
              }
          )
