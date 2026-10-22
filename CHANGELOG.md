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
- librdkafka 2.15 or newer is required and is found through pkg-config
  (`pkgconfig-depends: rdkafka >=2.15`) instead of `extra-libraries`, so an
  older library is rejected at configure time.
- `flushProducer` is implemented with `rd_kafka_flush` and still blocks until
  the outbound queue is empty.
- The unit test suite (`rdkafka-hs-test`) uses tasty instead of hspec. The
  integration test suite is now `rdkafka-hs-integration`, behind the manual
  `integration` flag (formerly `it`).

### Added

- `Kafka.Consumer.newAssignedConsumer` creates a consumer that never subscribes
  and so never joins its group; it reads only the partitions given to `assign`
  (improvement request IR-2).
- `Kafka.Consumer.pollMessageAttributed` and
  `Kafka.Consumer.Types.ConsumerMessageError` report the topic, partition and
  offset of an errored message, such as a partition EOF.
- `Kafka.Consumer.destroyConsumer` destroys the librdkafka client synchronously
  and exactly once, closing its broker connections before it returns.
- `Kafka.Consumer.setConsumerQueueIOEvent` asks librdkafka to write to a
  non-blocking file descriptor when records or events reach the queue
  `pollMessage` reads, in both callback poll modes, so applications can wait
  with `threadWaitRead` instead of polling on a timer (improvement request
  IR-1). `Kafka.Internal.RdKafka` binds `rd_kafka_queue_io_event_enable`,
  `rd_kafka_queue_cb_event_enable` and `rd_kafka_queue_get_consumer`.
- `Kafka.Producer.flushProducerTimeout` (`rd_kafka_flush`) waits a bounded time
  for the outbound queue to drain, and `Kafka.Producer.purgeProducer`
  (`rd_kafka_purge`) purges queued or in-flight messages, reporting each to the
  delivery callback with `RdKafkaRespErrPurgeQueue` or
  `RdKafkaRespErrPurgeInflight`.
- `Kafka.Consumer.assignmentLost` (`rd_kafka_assignment_lost`) and
  `Kafka.Consumer.consumerClosed` (`rd_kafka_consumer_closed`).
- `setSaslCredentials` in `Kafka.Consumer` and `Kafka.Producer`
  (`rd_kafka_sasl_set_credentials`) replaces a client's SASL credentials.
- `docs/bindings.md` inventories which librdkafka functions are bound, checked
  by `just bindings-check`.
- `Kafka.Version` reports the linked librdkafka version (`librdkafkaVersion`,
  `librdkafkaVersionNumber`, `librdkafkaVersionParts`).
- The integration suite runs against a project-local Redpanda broker on Apple
  Container (`just test-it`), selected by `RDKAFKA_TEST_BROKER` (default
  `127.0.0.1:39092`), and refuses the machine-global broker on port 9092. It
  includes a regression test that fences a static group member in both callback
  poll modes and checks the in-band fatal error and its cause.
- Admin API topic creation and deletion: `Kafka.Topic` and `Kafka.Topic.Types`
  (from unreleased upstream `main`).
- `consumerFatalError` reports the librdkafka fatal error raised on a consumer,
  if any, together with its reason.

### Fixed

- In `CallbackPollModeAsync`, records from partitions assigned with `assign`
  are delivered to `pollMessage`; previously the background callback-poll loop
  drained and discarded them.
- `closeConsumer` is idempotent and no longer leaves a released message queue
  reachable; polling messages destroys each message exactly once even if
  conversion fails.
- A raised consumer fatal error is reported in-band: `pollMessage` and
  `pollMessageBatch` return `KafkaResponseError RdKafkaRespErrFatal` in both
  callback poll modes instead of timing out indefinitely.
- The background callback-poll loop destroys the messages it consumes instead
  of leaking them.
- `storeOffsets` and `storeOffsetMessage` free the native partition list they
  allocate (from unreleased upstream `main`).
- Subscribing to an empty topic set no longer calls `rd_kafka_subscribe` (from
  unreleased upstream `main`).
