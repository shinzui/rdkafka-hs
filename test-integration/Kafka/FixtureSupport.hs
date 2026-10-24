-- | Test-only algorithms with injectable operations. Production fixtures and
-- harness self-tests use the same error policy and deadline handling.
module Kafka.FixtureSupport
  ( collectExactly,
    sendAndConfirm,
    acquirePair,
  )
where

import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import Control.Exception (bracketOnError)
import Data.Word (Word64)
import GHC.Clock (getMonotonicTimeNSec)
import Kafka.Producer

import System.Timeout (timeout)

-- | Only timeouts/EOF are idle. After the requested count, require an idle
-- poll so an unexpected extra record cannot be hidden by truncation.
collectExactly :: IO Word64 -> Word64 -> Int -> IO (Either KafkaError a) -> IO (Either KafkaError [a])
collectExactly clock budget count poll
  | count < 0 = pure (Left (KafkaError "negative expected record count"))
  | otherwise = do
      deadline <- (+ budget) <$> clock
      let go acc = do
            now <- clock
            if now >= deadline
              then pure (Left (KafkaError "record deadline expired before exact count and idle poll"))
              else
                poll >>= \case
                  Right record
                    | length acc == count -> pure (Left (KafkaError "unexpected extra record"))
                    | otherwise -> go (record : acc)
                  Left err
                    | idle err && length acc == count -> pure (Right (reverse acc))
                    | idle err -> go acc
                    | otherwise -> pure (Left err)
      go []
  where
    idle (KafkaResponseError RdKafkaRespErrTimedOut) = True
    idle (KafkaResponseError RdKafkaRespErrPartitionEof) = True
    idle _ = False

-- | Immediate rejection, delivery failure, missing callback, and failed flush
-- are distinct failures. One monotonic budget bounds the entire operation.
sendAndConfirm :: Int -> (ProducerRecord -> (DeliveryReport -> IO ()) -> IO (Either ImmediateError ())) -> (Timeout -> IO (Maybe KafkaError)) -> [ProducerRecord] -> IO (Either KafkaError ())
sendAndConfirm budget enqueue flush records = do
  deadline <- (+ fromIntegral budget * 1000) <$> getMonotonicTimeNSec
  let remaining = do
        now <- getMonotonicTimeNSec
        pure (if now >= deadline then 0 else fromIntegral ((deadline - now) `div` 1000))
      bounded action = remaining >>= \n -> if n <= 0 then pure Nothing else timeout n action
      go [] = pure (Right ())
      go (record : rest) = do
        report <- newEmptyMVar
        enqueue record (putMVar report) >>= \case
          Left (ImmediateError err) -> pure (Left err)
          Right () -> do
            micros <- remaining
            flush (Timeout (max 1 (micros `div` 1000))) >>= \case
              Just err -> pure (Left err)
              Nothing ->
                bounded (takeMVar report) >>= \case
                  Nothing -> pure (Left (KafkaError "delivery callback deadline expired"))
                  Just (DeliverySuccess actual _)
                    | sameRecord record actual -> go rest
                    | otherwise -> pure (Left (KafkaError "delivery report does not match enqueued record"))
                  Just (DeliveryFailure _ err) -> pure (Left err)
                  Just (NoMessageError err) -> pure (Left err)
  go records
  where
    sameRecord expected actual =
      prTopic expected == prTopic actual
        && prKey expected == prKey actual
        && prValue expected == prValue actual
        && prHeaders expected == prHeaders actual
        && case prPartition expected of
          UnassignedPartition -> True
          partition -> partition == prPartition actual

-- | Release the first resource when constructing the second fails, including
-- asynchronous exceptions. Successful pairs are released by the caller.
acquirePair :: IO a -> (a -> IO ()) -> IO b -> IO (a, b)
acquirePair first release second = bracketOnError first release $ \a -> (a,) <$> second
