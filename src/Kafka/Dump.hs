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
    peekCText,
    rdKafkaConfDump,
    rdKafkaConfDumpFree,
    rdKafkaConfPropertiesShow,
    rdKafkaDump,
    rdKafkaTopicConfDump,
    withCFile,
  )
import Kafka.Internal.Setup
  ( HasKafka (..),
    HasKafkaConf (..),
    HasTopicConf (..),
    getRdKafka,
    getRdKafkaConf,
    getRdTopicConf,
  )

import Control.Exception (bracket, evaluate)
import Control.Monad ((<=<))
import Control.Monad.IO.Class (MonadIO (liftIO))
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Text (Text)
import Foreign (Ptr, Storable (peek, peekElemOff), alloca, withForeignPtr)
import Foreign.C.String (CString)
import System.IO (Handle)

-- | Prints supported Kafka properties, flushing the native output before
-- returning. The handle stays open and owned by the caller. Serialize other
-- access to this handle while printing.
hPrintSupportedKafkaConf :: (MonadIO m) => Handle -> m ()
hPrintSupportedKafkaConf h = liftIO $ withCFile h "w" rdKafkaConfPropertiesShow

-- | Prints native client diagnostics, flushing before returning. The handle
-- stays open and owned by the caller. Serialize other access while printing.
hPrintKafka :: (MonadIO m, HasKafka k) => Handle -> k -> m ()
hPrintKafka h k = liftIO $ withForeignPtr (getRdKafka k) $ \_ ->
  withCFile h "w" (\file -> rdKafkaDump file (getRdKafka k))

-- | Returns a map of the current topic configuration
dumpTopicConf :: (MonadIO m, HasTopicConf t) => t -> m (Map Text Text)
dumpTopicConf t = liftIO $ parseDump (rdKafkaTopicConfDump (getRdTopicConf t))

-- | Returns a map of the current kafka configuration
dumpKafkaConf :: (MonadIO m, HasKafkaConf k) => k -> m (Map Text Text)
dumpKafkaConf k = liftIO $ parseDump (rdKafkaConfDump (getRdKafkaConf k))

parseDump :: (CSizePtr -> IO (Ptr CString)) -> IO (Map Text Text)
parseDump cstr = alloca $ \sizeptr ->
  bracket
    ( do
        pointer <- cstr sizeptr
        size <- peek sizeptr
        pure (pointer, size)
    )
    (uncurry rdKafkaConfDumpFree)
    ( \(pointer, size) -> do
        keysAndValues <- mapM (peekCText <=< peekElemOff pointer) [0 .. (fromIntegral size - 1)]
        evaluate (Map.fromList (listToTuple keysAndValues))
    )

listToTuple :: [Text] -> [(Text, Text)]
listToTuple [] = []
listToTuple (k : v : ts) = (k, v) : listToTuple ts
listToTuple _ = error "list to tuple can only be called on even length lists"
