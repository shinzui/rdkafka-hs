# Changelog

All notable changes to rdkafka-hs are recorded here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this project adheres to the [Haskell Package Versioning Policy](https://pvp.haskell.org/).

## [Unreleased]

## [1.1.0.0] - 2026-10-24

### Changed

- Later subscription property values now override earlier values. Topic names
  still accumulate by set union. This changes the inherited precedence rule.
- Producer topic settings now take effect; explicit topic values override
  matching global values. Empty keys and values remain distinct from absent ones.
- Consumer, seek, metadata and administration calls preserve native failures,
  including per-partition errors. Embedded NUL names and out-of-range numeric
  inputs are rejected before changing state.
- Callback exceptions are contained in Haskell and reported through the runtime
  exception handler. Async consumers require a threaded runtime; sync consumers
  also work in nonthreaded executables.

### Fixed

- Topic creation applies its configuration without the inherited crash. Admin
  requests retain their actual deadline and cannot report malformed responses
  as success.
- Manual synchronous callback polling preserves ordered records. Successful
  seek/rebalance operations discard the appropriate stale buffered state;
  failed native state changes preserve it and report their errors.
- Native clients, callbacks, message batches, metadata/group results,
  transaction errors and partition lists release their owners across failure,
  cancellation and teardown. Temporary seek topics are destroyed before their
  native parent, preventing late finalizers from corrupting another consumer.
- Diagnostic output flushes correctly, preserves the caller's handle and closes
  temporary native streams and duplicate descriptors on errors.
- SASL credential replacement preserves Unicode identity, rejects embedded NUL
  and releases native errors. Native producer error codes remain intact, and
  rejected per-message callback roots are released before error decoding.

Validation includes full primary normal/coverage runs and named Linux, Apache
failover, native lifetime and authenticated TLS/ACL/reconnect scenarios. The
remaining comprehensive qualification work stays tracked in
[MasterPlan 3](docs/masterplans/3-complete-deferred-coverage-and-compatibility-validation.md).

## [1.0.0.1] - 2026-10-23

### Fixed

- Consumed record headers were decoded in reverse wire order, so
  `headersToList` returned the last header first and selecting the last
  occurrence of a repeated name picked the first value the producer sent.
  `pollMessage`, `pollMessageAttributed` and every other consumer path now
  return the headers in the order the producer sent them, repeated names and
  identical pairs included. The `Headers` API is unchanged.
- The `setConsumerQueueIOEvent` documentation described a wait protocol that
  discarded pending wake-ups after draining the queue, which can lose the
  wake-up for a record that arrives in between. It now discards them first.

## [1.0.0.0] - 2026-10-22

rdkafka-hs is the maintained successor of
[hw-kafka-client](https://github.com/haskell-works/hw-kafka-client). Its history
starts from hw-kafka-client 5.3.0 (Hackage, 2023), includes the twelve commits on
upstream `main` that were never released (up to `56be4a6`, 2025-10-03), and adds
the consumer fatal-error fix `6caed63`. Everything below is relative to Hackage's
hw-kafka-client 5.3.0. See the README's "Migrating from hw-kafka-client".

### Changed

- The package is renamed from `hw-kafka-client` to `rdkafka-hs` and its version
  restarts at `1.0.0.0`. The `Kafka.*` module namespace, public type names and
  record field names are unchanged, so dependents migrate by changing the
  dependency name and version bounds in `build-depends`, not their imports.
- GHC 9.12 or newer is required. Every component uses the `GHC2024` language
  edition with `DeriveAnyClass`, `DuplicateRecordFields`, `OverloadedLabels` and
  `OverloadedStrings` enabled, and every `deriving` clause names its strategy.
- librdkafka 2.15 or newer is required; building against an older header stops
  with `rdkafka-hs requires librdkafka 2.15 or newer`.
- `cpCallbackPollMode` is now `Maybe CallbackPollMode` (`Nothing` until
  `callbackPollMode` sets it); `effectiveCallbackPollMode` returns the mode a
  consumer uses. See the poll-mode fix below.
- `flushProducer` is implemented with `rd_kafka_flush` and still blocks until
  the outbound queue is empty.
- `closeConsumer` is idempotent: closing again, or after `destroyConsumer`,
  does nothing and returns `Nothing`.
- The unit test suite (`rdkafka-hs-test`) uses tasty instead of hspec. The
  integration test suite is now `rdkafka-hs-integration`, behind the manual
  `integration` flag (formerly `it`).

### Added

- From unreleased upstream `main`: admin API topic creation and deletion,
  `Kafka.Topic` and `Kafka.Topic.Types`.
- `Kafka.Consumer.consumerFatalError` returns the fatal error raised on a
  consumer, if any, with librdkafka's description.
- `Kafka.Consumer.setConsumerQueueIOEvent` asks librdkafka to write to a
  non-blocking file descriptor when records or events reach the queue
  `pollMessage` reads, in both callback poll modes, so applications can wait
  with `threadWaitRead` instead of polling on a timer (improvement request
  IR-1). `Kafka.Internal.RdKafka` binds `rd_kafka_queue_io_event_enable`,
  `rd_kafka_queue_cb_event_enable` and `rd_kafka_queue_get_consumer`.
- For bounded, read-only observers (improvement request IR-2):
  - `Kafka.Consumer.newAssignedConsumer` creates a consumer that never
    subscribes and so never joins its group; it reads only the partitions
    given to `assign`.
  - `Kafka.Consumer.pollMessageAttributed` and
    `Kafka.Consumer.Types.ConsumerMessageError` report the topic, partition and
    offset of an errored message, such as a partition EOF.
  - `Kafka.Consumer.destroyConsumer` destroys the librdkafka client
    synchronously and exactly once, closing its broker connections before it
    returns.
- `Kafka.Producer.flushProducerTimeout` (`rd_kafka_flush`) waits a bounded time
  for the outbound queue to drain, and `Kafka.Producer.purgeProducer`
  (`rd_kafka_purge`, with `PurgeFlag`) purges queued or in-flight messages,
  reporting each to the delivery callback with `RdKafkaRespErrPurgeQueue` or
  `RdKafkaRespErrPurgeInflight`.
- `Kafka.Consumer.assignmentLost` (`rd_kafka_assignment_lost`) and
  `Kafka.Consumer.consumerClosed` (`rd_kafka_consumer_closed`).
- `setSaslCredentials` in `Kafka.Consumer` and `Kafka.Producer`
  (`rd_kafka_sasl_set_credentials`) replaces a client's SASL credentials.
- `Kafka.Version` reports the linked librdkafka version (`librdkafkaVersion`,
  `librdkafkaVersionNumber`, `librdkafkaVersionParts`).
- `docs/bindings.md` inventories which of librdkafka's exported functions are
  bound, checked by `just bindings-check`.
- The integration suite runs against a project-local Redpanda broker on Apple
  Container (`just test-it`), selected by `RDKAFKA_TEST_BROKER` (default
  `127.0.0.1:39092`), and refuses the machine-global broker on port 9092. It
  includes a regression test that fences a static group member in both callback
  poll modes and checks the in-band fatal error and its cause.

### Fixed

- A raised consumer fatal error is reported in-band: `pollMessage`,
  `pollMessageBatch` and `pollMessageAttributed` return
  `KafkaResponseError RdKafkaRespErrFatal` in both callback poll modes, on every
  call, instead of the error being unobservable in `CallbackPollModeAsync`.
- The background callback-poll loop destroys the messages it consumes instead
  of leaking them.
- In `CallbackPollModeAsync`, records from partitions assigned with `assign`
  are delivered to `pollMessage`, including after reassignment; previously the
  background callback-poll loop drained and discarded them.
- Composing `ConsumerProperties` no longer resets the callback poll mode:
  every builder used to carry the default `CallbackPollModeAsync` and the
  semigroup took the right operand's mode, so
  `callbackPollMode CallbackPollModeSync <> groupId g` silently produced an
  async consumer. A mode now changes only when `callbackPollMode` sets it.
- `closeConsumer` no longer leaves a released message queue reachable, and
  polling destroys each message exactly once even if conversion fails.
- From unreleased upstream `main`: `storeOffsets` and `storeOffsetMessage` free
  the native partition list they allocate, and subscribing to an empty topic
  set no longer calls `rd_kafka_subscribe`.

### Removed

- The niv-based Nix files (`default.nix`, `shell.nix`, `nix/sources.*`),
  `docker-compose.yml`, the GitHub Actions workflow, `Setup.hs` and the
  `build/` and `scripts/` helpers of hw-kafka-client; the seihou-managed flake
  replaces them.
- The `it` Cabal flag (renamed `integration`) and the hspec test suites.
