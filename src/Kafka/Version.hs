-- | The version of the librdkafka C library this program is linked against.
--
-- rdkafka-hs requires librdkafka 2.15 or newer; the build refuses an older
-- library at configure time. These values come from the library loaded at run
-- time, which can be newer than the headers the package was compiled with.
module Kafka.Version
  ( LibrdkafkaVersion (..),
    librdkafkaVersion,
    librdkafkaVersionNumber,
    librdkafkaVersionParts,
    decodeLibrdkafkaVersion,
  )
where

import Data.Bits (shiftR, (.&.))
import Data.Text (Text)
import Data.Text qualified as Text
import Kafka.Internal.RdKafka (rdKafkaVersion, rdKafkaVersionStr)

-- | A decoded librdkafka version number.
data LibrdkafkaVersion = LibrdkafkaVersion
  { versionMajor :: Int,
    versionMinor :: Int,
    versionRevision :: Int
  }
  deriving stock (Eq, Ord, Show)

-- | The linked library's version string, for example @"2.15.0"@
-- (@rd_kafka_version_str@).
librdkafkaVersion :: Text
librdkafkaVersion = Text.pack rdKafkaVersionStr

-- | The linked library's version number in librdkafka's hex encoding
-- @0xMMmmrrpp@ (major, minor, revision, pre-release), for example
-- @0x020f00ff@ for 2.15.0 (@rd_kafka_version@).
librdkafkaVersionNumber :: Int
librdkafkaVersionNumber = rdKafkaVersion

-- | The linked library's version, decoded from 'librdkafkaVersionNumber'.
librdkafkaVersionParts :: LibrdkafkaVersion
librdkafkaVersionParts = decodeLibrdkafkaVersion librdkafkaVersionNumber

-- | Decode librdkafka's @0xMMmmrrpp@ version encoding. The pre-release byte is
-- ignored.
decodeLibrdkafkaVersion :: Int -> LibrdkafkaVersion
decodeLibrdkafkaVersion n =
  LibrdkafkaVersion
    { versionMajor = (n `shiftR` 24) .&. 0xff,
      versionMinor = (n `shiftR` 16) .&. 0xff,
      versionRevision = (n `shiftR` 8) .&. 0xff
    }
