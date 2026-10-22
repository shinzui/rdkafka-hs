-----------------------------------------------------------------------------

-----------------------------------------------------------------------------

-- |
-- Module providing various functions to dump information. These may be useful for
-- debug/investigation but should probably not be used on production applications.
module Kafka.Dump
  ( hPrintSupportedKafkaConf,
    hPrintKafka,
    dumpKafkaConf,
    dumpTopicConf,
  )
where

import Kafka.Internal.RdKafka
  ( CSizePtr,
    handleToCFile,
    peekCText,
    rdKafkaConfDump,
    rdKafkaConfDumpFree,
    rdKafkaConfPropertiesShow,
    rdKafkaDump,
    rdKafkaTopicConfDump,
  )
import Kafka.Internal.Setup
  ( HasKafka (..),
    HasKafkaConf (..),
    HasTopicConf (..),
    getRdKafka,
    getRdKafkaConf,
    getRdTopicConf,
  )

import Control.Monad ((<=<))
import Control.Monad.IO.Class (MonadIO (liftIO))
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Text (Text)
import Foreign (Ptr, Storable (peek, peekElemOff), alloca)
import Foreign.C.String (CString)
import System.IO (Handle)

-- | Prints out all supported Kafka conf properties to a handle
hPrintSupportedKafkaConf :: (MonadIO m) => Handle -> m ()
hPrintSupportedKafkaConf h = liftIO $ handleToCFile h "w" >>= rdKafkaConfPropertiesShow

-- | Prints out all data associated with a specific kafka object to a handle
hPrintKafka :: (MonadIO m, HasKafka k) => Handle -> k -> m ()
hPrintKafka h k = liftIO $ handleToCFile h "w" >>= \f -> rdKafkaDump f (getRdKafka k)

-- | Returns a map of the current topic configuration
dumpTopicConf :: (MonadIO m, HasTopicConf t) => t -> m (Map Text Text)
dumpTopicConf t = liftIO $ parseDump (rdKafkaTopicConfDump (getRdTopicConf t))

-- | Returns a map of the current kafka configuration
dumpKafkaConf :: (MonadIO m, HasKafkaConf k) => k -> m (Map Text Text)
dumpKafkaConf k = liftIO $ parseDump (rdKafkaConfDump (getRdKafkaConf k))

parseDump :: (CSizePtr -> IO (Ptr CString)) -> IO (Map Text Text)
parseDump cstr = alloca $ \sizeptr -> do
  strPtr <- cstr sizeptr
  size <- peek sizeptr

  keysAndValues <- mapM (peekCText <=< peekElemOff strPtr) [0 .. (fromIntegral size - 1)]

  let ret = Map.fromList $ listToTuple keysAndValues
  rdKafkaConfDumpFree strPtr size
  return ret

listToTuple :: [Text] -> [(Text, Text)]
listToTuple [] = []
listToTuple (k : v : ts) = (k, v) : listToTuple ts
listToTuple _ = error "list to tuple can only be called on even length lists"
