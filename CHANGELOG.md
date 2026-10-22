# Changelog

All notable changes to rdkafka-hs are recorded here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this project adheres to the [Haskell Package Versioning Policy](https://pvp.haskell.org/).

rdkafka-hs is the maintained successor of
[hw-kafka-client](https://github.com/haskell-works/hw-kafka-client). Its history
starts from hw-kafka-client 5.3.0 (Hackage, 2023), includes the twelve commits on
upstream `main` that were never released (up to `56be4a6`, 2025-10-03), and adds
the consumer fatal-error fix `6caed63`. Everything below is relative to Hackage's
hw-kafka-client 5.3.0.

## [Unreleased]

### Changed

- The package is renamed from `hw-kafka-client` to `rdkafka-hs` and its version
  restarts at `1.0.0.0`. The `Kafka.*` module namespace, public type names and
  record field names are unchanged, so dependents migrate by changing the
  dependency name and bounds in `build-depends`.
- GHC 9.12 or newer is required. Every component uses the `GHC2024` language
  edition with `DeriveAnyClass`, `DuplicateRecordFields`, `OverloadedLabels` and
  `OverloadedStrings` enabled, and every `deriving` clause names its strategy.
- The unit test suite (`rdkafka-hs-test`) uses tasty instead of hspec. The
  integration test suite is now `rdkafka-hs-integration`, behind the manual
  `integration` flag (formerly `it`).

### Added

- Admin API topic creation and deletion: `Kafka.Topic` and `Kafka.Topic.Types`
  (from unreleased upstream `main`).
- `consumerFatalError` reports the librdkafka fatal error raised on a consumer,
  if any, together with its reason.

### Fixed

- A raised consumer fatal error is reported in-band: `pollMessage` and
  `pollMessageBatch` return `KafkaResponseError RdKafkaRespErrFatal` in both
  callback poll modes instead of timing out indefinitely.
- The background callback-poll loop destroys the messages it consumes instead
  of leaking them.
- `storeOffsets` and `storeOffsetMessage` free the native partition list they
  allocate (from unreleased upstream `main`).
- Subscribing to an empty topic set no longer calls `rd_kafka_subscribe` (from
  unreleased upstream `main`).
