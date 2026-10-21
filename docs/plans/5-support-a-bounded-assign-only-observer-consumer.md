---
id: 5
slug: support-a-bounded-assign-only-observer-consumer
title: "Support a bounded assign-only observer consumer"
kind: exec-plan
created_at: 2026-10-21T23:17:55Z
intention: "intention_01m5gc388wecfsq431s3pgg1dd"
master_plan: "docs/masterplans/1-fork-hw-kafka-client-as-rdkafka-hs.md"
provenance:
  created_by:
    model: "claude-fable-5-1"
    harness: "claude-code"
    at: 2026-10-21T23:17:55Z
---

# Support a bounded assign-only observer consumer

This ExecPlan is a living document. The sections Progress, Surprises & Discoveries,
Decision Log, and Outcomes & Retrospective must be kept up to date as work proceeds.
If durable project context changes, update or create ADRs in docs/adr/ in the same change.


## Purpose / Big Picture

After this plan, an application can build a read-only Kafka *observer*: a consumer that
reads chosen partitions of a topic from a known offset to each partition's end, never
joins a consumer group, never commits offsets, can tell which partition each
end-of-partition marker or per-message error belongs to, and is gone (its sockets closed)
the moment the application says so rather than whenever the garbage collector runs. Four
things become possible that are not today: `newAssignedConsumer` creates a consumer that
never subscribes; `pollMessageAttributed` returns errors together with the topic,
partition and offset librdkafka attached to them; `destroyConsumer` tears the client down
synchronously and exactly once; and a test pins the fact that watermark queries on a
`read_committed` consumer stop at the last stable offset while a transaction is open. This
closes improvement request IR-2 in
`docs/improvement-requests/2-support-a-bounded-assign-only-observer-consumer.md`, whose
acceptance criteria AC-1 through AC-4 are this plan's acceptance criteria; AC-5 (a tagged
commit) is met by `docs/plans/6-cut-the-first-rdkafka-hs-release-and-migration-guide.md`.
The consumer is `mori://shinzui/shoko`'s Kafka topic evidence adapter
(`mori://shinzui/shoko/plans/10-implement-the-kafka-topic-evidence-adapter`).


## Progress

- [ ] M1: `newAssignedConsumer` exists and an assign-only consumer with a fresh group id
      receives records without the group appearing in the broker's group list, in both
      poll modes (IR-2 AC-1).
- [ ] M2: `pollMessageAttributed` and `ConsumerMessageError` exist; with
      `enable.partition.eof=true` a consumer assigned two partitions receives exactly one
      EOF per partition naming it, with the busy partition's EOF offset equal to its end
      offset (IR-2 AC-2).
- [ ] M3: `destroyConsumer` closes sockets before it returns, is safe to call twice, and
      leaves the finalizer a no-op (IR-2 AC-3); ADR-5 on explicit consumer teardown
      written.
- [ ] M4: `read_committed` watermark test passes (IR-2 AC-4); IR-2 moved to `completed`;
      CHANGELOG updated.


## Surprises & Discoveries

(None yet.)


## Decision Log

- Decision: `pollMessage` keeps its type; attributed errors come from a new function.
  Rationale: IR-2 asks for it explicitly, and every downstream consumer pattern-matches on
  `Either KafkaError (ConsumerRecord ...)`.
  Date: 2026-10-21

- Decision: `destroyConsumer` performs `closeConsumer` first and then runs the existing
  `rd_kafka_destroy_flags` finalizer through `finalizeForeignPtr`, guarded by a
  destroyed flag in `KafkaConf`, rather than introducing a second destroy path.
  Rationale: IR-2's proposed shape; one finalizer means one place where the handle is
  freed, and the flag is what makes a second call safe instead of a use-after-free.
  Date: 2026-10-21


## Outcomes & Retrospective

(To be filled during and after implementation.)


## Context and Orientation

`rdkafka-hs` binds librdkafka, the C Kafka client. The pieces this plan touches:

`src/Kafka/Consumer.hs` exports `newConsumer :: MonadIO m => ConsumerProperties ->
Subscription -> m (Either KafkaError KafkaConsumer)`. Since the inherited upstream commit
`56be4a6` it calls `rd_kafka_subscribe` only when the subscription's topic set is
non-empty, so `newConsumer props mempty` already yields a consumer that has not joined a
group; nothing documents or tests that, and `Subscription` (`src/Kafka/Consumer/Subscription.hs`)
is a `Monoid` whose `mempty` has no topics. `assign :: MonadIO m => KafkaConsumer ->
[TopicPartition] -> m (Maybe KafkaError)` assigns partitions with explicit offsets
(`TopicPartition` and `PartitionOffset` are in `src/Kafka/Consumer/Types.hs`). librdkafka
still requires `group.id` for a high-level consumer even when it never subscribes, because
`newConsumer` calls `rd_kafka_poll_set_consumer`, which needs the consumer group object;
with no subscribe and `enable.auto.commit=false` (the `noAutoCommit` property) the client
never sends a JoinGroup or OffsetCommit, so the broker never records the group.

`pollMessage` returns `Either KafkaError (ConsumerRecord (Maybe ByteString) (Maybe
ByteString))`. The conversion is `Kafka.Consumer.Convert.fromMessagePtr`
(`src/Kafka/Consumer/Convert.hs`): it peeks the `rd_kafka_message_t` struct
(`RdKafkaMessageT` in `src/Kafka/Internal/RdKafka.chs`, with fields `err'`, `topic'` (a
`Ptr RdKafkaTopicT`), `partition'`, `len'`, `keyLen'`, `offset'`, `payload'`, `key'`,
`opaque'`), and when `err'` is not `RdKafkaRespErrNoError` returns
`Left (KafkaResponseError err)` and destroys the message, dropping the topic, partition
and offset librdkafka set on it. With the consumer property `enable.partition.eof=true`
librdkafka delivers one message with `err' = RdKafkaRespErrPartitionEof` per partition
when the fetch reaches the end, carrying that partition and the end offset; today those
arrive as anonymous `Left` values. `rd_kafka_topic_name` is bound (`rdKafkaTopicName`) and
reads the topic name from the topic handle in the struct.

`closeConsumer` (`src/Kafka/Consumer.hs`) takes the callback-poll status `MVar` (which
stops the async background loop), destroys the async message queue, and calls
`rd_kafka_consumer_close`. It does not destroy the client. The client handle
`RdKafkaTPtr` is a `ForeignPtr` created in `Kafka.Internal.RdKafka.newRdKafkaT`, which
attaches, through `Foreign.Concurrent.addForeignPtrFinalizer`, a finalizer calling
`rd_kafka_destroy_flags(rk, RD_KAFKA_DESTROY_F_NO_CONSUMER_CLOSE)`; it runs whenever the
garbage collector drops the pointer, so broker connections and threads outlive
`closeConsumer` by an unbounded time. `Foreign.ForeignPtr.finalizeForeignPtr` runs a
foreign pointer's finalizers immediately and exactly once; afterwards the pointer must
never be touched again. `KafkaConf` (`src/Kafka/Internal/Setup.hs`) is the record carrying
the config pointer, the message-queue `IORef` and the status `MVar`; it is internal, so
adding a field is not an API change. `docs/plans/3-refresh-the-librdkafka-bindings-for-2-15-and-newer.md`
added `consumerClosed :: KafkaConsumer -> m Bool` (from `rd_kafka_consumer_closed`).

`Kafka.Metadata.partitionWatermarkOffsets` calls `rd_kafka_query_watermark_offsets`,
which librdkafka documents as honouring the handle's `isolation.level`: on a
`read_committed` consumer the high watermark reported is the last stable offset, which
stops before an open transaction. `Kafka.Transaction` (`src/Kafka/Transaction.hs`)
provides `initTransactions`, `beginTransaction`, `commitTransaction` and
`abortTransaction` for a producer configured with `transactional.id`. `Kafka.Metadata` also
provides `allConsumerGroupsInfo`, which lists the broker's consumer groups through
`rd_kafka_list_groups`.

Tests: `rdkafka-hs-integration` (tasty, flag `integration`) runs via `just test-it`
against the project-local Redpanda at `RDKAFKA_TEST_BROKER` (default `127.0.0.1:39092`),
with fixtures in `test-integration/Kafka/TestEnv.hs` and groups registered in
`test-integration/Main.hs` (from `docs/plans/2-run-integration-tests-on-a-project-local-redpanda.md`).
Redpanda supports transactions and `read_committed` isolation. The socket test for
`destroyConsumer` needs the `network` package in the integration suite.

ADR context: `docs/adr/4-consumer-fatal-errors-are-surfaced-in-band.md` (from EP-2)
requires every poll path to report a raised fatal in-band; `pollMessageAttributed` must
call the same fatal check as `pollMessage`. Cross-repository,
`mori://shinzui/keiro/okf/adrs/concepts/ADR-11` already states that "consumer close is
scope-sequenced rather than left to the garbage collector" from the application's side;
this plan writes the library-side ADR-5. The request's origin is
`mori://shinzui/shoko/plans/10-implement-the-kafka-topic-evidence-adapter`.


## Plan of Work

**Milestone 1: assign-only construction.** Add and export from `Kafka.Consumer`:

```haskell
-- | Create a consumer that never subscribes and therefore never joins a group.
-- Partitions are read only after 'assign'. A group.id is still required by
-- librdkafka; with 'noAutoCommit' the broker never records the group.
newAssignedConsumer :: MonadIO m => ConsumerProperties -> m (Either KafkaError KafkaConsumer)
newAssignedConsumer props = newConsumer props mempty
```

Document on `newConsumer` that an empty subscription skips `rd_kafka_subscribe`. Add
`test-integration/Kafka/Integration/AssignOnlyTest.hs`, one case per poll mode: create a
prefixed topic with one partition; produce one record and flush; create an assigned
consumer with a fresh prefixed group id, `noAutoCommit` and `offsetReset Earliest`;
`assign` partition 0 at `PartitionOffsetBeginning`; poll until the record arrives (bounded
by ten seconds); then `allConsumerGroupsInfo` must not contain the group id; close the
consumer.

**Milestone 2: attributed errors.** In `src/Kafka/Consumer/Types.hs` add and export:

```haskell
data ConsumerMessageError = ConsumerMessageError
  { cmeError     :: !KafkaError
  , cmeTopic     :: !(Maybe TopicName)
  , cmePartition :: !(Maybe PartitionId)
  , cmeOffset    :: !(Maybe Offset)
  }
  deriving stock (Eq, Show)
```

In `src/Kafka/Consumer/Convert.hs` add `fromMessagePtrAttributed :: RdKafkaMessageTPtr ->
IO (Either ConsumerMessageError (ConsumerRecord ...))`: a null pointer (timeout) yields
the errno-derived error with all origin fields `Nothing`; an errored message yields the
code plus the topic name (via `rdKafkaTopicName` when the topic pointer is non-null),
partition and offset, then destroys the message; a good message is converted as today.
Refactor `fromMessagePtr` to share the record-building code. In `Kafka.Consumer` add and
export `pollMessageAttributed :: MonadIO m => KafkaConsumer -> Timeout -> m (Either
ConsumerMessageError (ConsumerRecord (Maybe ByteString) (Maybe ByteString)))`, which runs
the same fatal-error check as `pollMessage` first (returning an attributed
`RdKafkaRespErrFatal` with no origin) and then polls the same queue `pollMessage` would.
Add `test-integration/Kafka/Integration/AttributedErrorTest.hs`: create a prefixed topic
with two partitions; produce one record to partition 1 and flush; create an assigned
consumer with `extraProps` setting `enable.partition.eof=true` and `noAutoCommit`;
assign both partitions at `PartitionOffsetBeginning`; poll with `pollMessageAttributed`
until two EOF errors have been seen or fifteen seconds pass, collecting them; assert
exactly one EOF names partition 0 at offset 0 and exactly one names partition 1 at
offset 1 (the end offset after one record), both carrying the topic name, and that the
record from partition 1 also arrived.

**Milestone 3: deterministic destroy.** Add a destroyed flag to `KafkaConf` (an `IORef
Bool` or an `MVar`), initialised false in `newKafkaConf`. In `Kafka.Consumer` add and
export:

```haskell
-- | Close the consumer and destroy the underlying librdkafka client now,
-- synchronously and exactly once. Every other operation on this consumer is
-- invalid afterwards. Safe to call more than once; later calls do nothing.
destroyConsumer :: MonadIO m => KafkaConsumer -> m ()
```

Implementation: atomically test-and-set the flag; if it was already set, return. Otherwise
run `closeConsumer` (it disables the callback loop, destroys the message queue and leaves
the group), then `finalizeForeignPtr` on the client pointer, which runs the registered
`rd_kafka_destroy_flags` finalizer immediately; because `Foreign.Concurrent` finalizers
are Haskell actions, `finalizeForeignPtr` runs them on the calling thread and the
re-entrancy concern that motivated the concurrent finalizer (callbacks during destroy)
is handled by `closeConsumer` having already disabled the loop. Make `closeConsumer`
itself a no-op when the flag is set so `bracket`-style code that closes after destroying
does not touch a freed handle. Add `test-integration/Kafka/Integration/DestroyTest.hs`:
open a listening socket on `127.0.0.1` port 0 with `Network.Socket`, accept connections on
a thread that reads and never writes; create a consumer whose `bootstrap.servers` is that
address (librdkafka connects immediately); poll once with a 500 ms timeout; call
`destroyConsumer`; the accepting thread must observe `recv` return an empty string
(end-of-file) within one second of the call; call `destroyConsumer` again and assert it
returns normally. Write `docs/adr/5-consumer-teardown-is-explicit-and-deterministic.md`:
`closeConsumer` leaves the group, `destroyConsumer` frees the client now, the finalizer
remains only as a safety net, and why a second destroy path was rejected.

**Milestone 4: read-committed watermarks.** Add
`test-integration/Kafka/Integration/ReadCommittedTest.hs`: create a prefixed topic with one
partition; create a transactional producer (`transactional.id` set to a prefixed value
through the producer's `extraProps`), `initTransactions`, `beginTransaction`, produce one
record, flush; create two assigned consumers on the topic, one with `isolation.level=
read_committed` and one with `read_uncommitted`; `partitionWatermarkOffsets` on each must
show the read-committed high offset strictly below the read-uncommitted one while the
transaction is open; `commitTransaction`; re-query until the read-committed high offset
equals the read-uncommitted one (bounded by ten seconds). Then edit
`docs/improvement-requests/2-support-a-bounded-assign-only-observer-consumer.md`: `status:
completed`, a "Resolution" section naming the four functions and test modules and noting
AC-5's tag comes from EP-6, plus a dated `docs/improvement-requests/log.md` entry. Append
the CHANGELOG entries under `Unreleased` / `Added`.


## Concrete Steps

All commands run from `/Users/shinzui/Keikaku/bokuno/rdkafka-hs` inside the dev shell.

```bash
just redpanda-up
cabal build rdkafka-hs
cabal test rdkafka-hs-integration -f integration --test-options='-p "/assign-only/"'
cabal test rdkafka-hs-integration -f integration --test-options='-p "/attributed/"'
cabal test rdkafka-hs-integration -f integration --test-options='-p "/destroy/"'
cabal test rdkafka-hs-integration -f integration --test-options='-p "/read_committed/"'
just test-it
nix build
okf validate docs/adr --strict --profile docs/adr/profile.dhall --profile-enforce --log-enforce
okf validate docs/improvement-requests --strict \
  --profile docs/improvement-requests/profile.dhall --profile-enforce --log-enforce
```

Each focused run prints its cases passing (two for assign-only, one or two for the
others). To watch the group assertion from outside the test, while the assign-only test is
paused or immediately after it, `rpk group list -X brokers=127.0.0.1:39092` must not show
the run-prefixed group. Every commit carries:

```text
MasterPlan: docs/masterplans/1-fork-hw-kafka-client-as-rdkafka-hs.md
ExecPlan: docs/plans/5-support-a-bounded-assign-only-observer-consumer.md
Intention: intention_01m5gc388wecfsq431s3pgg1dd
```


## Validation and Acceptance

Accepted when IR-2's AC-1 through AC-4 pass exactly as the request states them, through
the four test modules above under `just test-it`; `cabal build all -f examples` is
warning-free; `nix build` passes; `okf validate` accepts both bundles; and the
`destroyConsumer` haddock and ADR-5 state that all other operations are invalid after it.
The behaviour is visible beyond the tests: in the socket test transcript the accepted
connection reaches end-of-file within one second of `destroyConsumer`, whereas with
`closeConsumer` alone it stays open until a garbage collection.

Required acceptance is M1 through M4. A `bracket`-style helper
(`withAssignedConsumer`) is welcome but not required; adding it is in scope only if it
costs one function.


## Idempotence and Recovery

All edits are additive; `pollMessage` and `closeConsumer` keep their types and, apart from
the destroyed-flag guard, their behaviour. The tests use run-prefixed names and create
their own topics, so reruns need no cleanup. If the EOF assertions are flaky because
Redpanda delivers the EOF for the empty partition before assignment settles, poll longer
rather than loosening the exact-count assertion; the count is the point of AC-2. If the
socket test cannot observe end-of-file on macOS because `recv` blocks, use a non-blocking
socket with a bounded `timeout`; the one-second bound is part of AC-3. If `finalizeForeignPtr`
proves unable to run a `Foreign.Concurrent` finalizer synchronously on the current GHC,
record it in Surprises & Discoveries and replace the finalizer with one from
`Foreign.ForeignPtr` paired with an explicit `rd_kafka_destroy_flags` call guarded by the
same flag; the ADR must then be amended.


## Interfaces and Dependencies

Prerequisites: `docs/plans/2-run-integration-tests-on-a-project-local-redpanda.md`
Complete (fixtures, `just test-it`, the private broker) and
`docs/plans/3-refresh-the-librdkafka-bindings-for-2-15-and-newer.md` Complete
(`consumerClosed`, binding conventions). Soft: `docs/plans/4-expose-queue-event-notification-for-consumers.md`
edits the same `Kafka.Consumer` export list and `test-integration/Main.hs`; whichever
lands second rebases.

Libraries: `network` added to the integration suite for the socket test; `unix` and
`base` otherwise.

Interfaces defined here, consumed by `mori://shinzui/shoko`: `Kafka.Consumer.newAssignedConsumer`,
`Kafka.Consumer.pollMessageAttributed`, `Kafka.Consumer.Types.ConsumerMessageError(..)`,
and `Kafka.Consumer.destroyConsumer`, all with the types shown above.
