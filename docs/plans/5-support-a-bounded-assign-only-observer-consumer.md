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
  reviews:
    - model: "gpt-6-astra"
      harness: "codex-cli"
      at: 2026-10-21T23:52:39Z
      verdict: "changes-requested"
      note: "Changes requested: manual async assignment lacks queue forwarding; destroyed flag skips close and permits incomplete concurrent teardown."
  revisions:
    - model: "gpt-6-astra"
      harness: "codex-cli"
      at: 2026-10-22T00:00:20Z
      mode: "update"
      note: "Repair async manual assignment and serialized bounded consumer destruction."
    - model: "claude-opus-5-5"
      harness: "claude-code"
      at: 2026-10-22T01:00:55Z
      mode: "implement"
      note: "Implement EP-5: assign-only routing, attributed errors, deterministic destroy, read_committed test."
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

- [x] M1 (2026-10-22): `newAssignedConsumer` exists and an assign-only consumer with a fresh group id
      receives records without the group appearing in the broker's group list, in both
      poll modes (IR-2 AC-1). Evidence: `assign-only / async` and `/ sync` pass for both
      constructors, including reassignment at offset 1. Before the routing change both
      async cases failed (`no record arrived within 10 seconds`); forwarding only
      before `rd_kafka_assign` still lost the reassigned record (`expected r1`). Commit
      `7a89012`.
- [x] M2 (2026-10-22): `pollMessageAttributed` and `ConsumerMessageError` exist; with
      `enable.partition.eof=true` a consumer assigned two partitions receives exactly one
      EOF per partition naming it, with the busy partition's EOF offset equal to its end
      offset (IR-2 AC-2). Evidence: `attributed / async: OK (3.53s)`, `/ sync: OK (3.50s)`;
      the fatal test also checks the attributed fatal result. Commit `bb38ead`.
- [x] M3 (2026-10-22): `destroyConsumer` closes sockets before it returns, is safe to call twice, and
      leaves the finalizer a no-op (IR-2 AC-3); the consumer queue and teardown ADR updated.
      Evidence: against a silent listener, `destroyConsumer returned after 49 ms; all 1
      connection(s) closed after 49 ms` (async) and `0 ms` (sync); repeated,
      concurrent, interrupted, close-then-destroy and destroy-then-close cases pass in
      both modes; no GC is forced. Commit `0ce5ece`.
- [x] M4 (2026-10-22): `read_committed` watermark test passes (IR-2 AC-4); IR-2 implementation evidence recorded, release criterion still open;
      CHANGELOG updated. Evidence: `read_committed watermark stops before an open
      transaction: OK (1.35s)`; the full suite reports `All 96 tests passed (81.24s)`;
      `cabal build all -f examples --enable-tests` and the integration build are
      warning-free; `just check` passes.


## Surprises & Discoveries

Review (2026-10-21): the imported `assign` omits async forwarding, which is only
performed by the rebalance callback. The previous destroyed flag would cause guarded
close to skip cleanup, and graceful close would weaken bounded destruction. The updated
plan follows recovered IR-2 and requires serialized, exception-safe cleanup. Source
inspection confirms query-watermark isolation is passed to ListOffsets in v2.15.0.

Implementation (2026-10-22):

- Forwarding before `rd_kafka_assign` is not enough on reassignment. librdkafka
  2.15 stops a reassigned partition's fetcher and clears its fetch queue's
  forwarding when the stop is processed (`rd_kafka_toppar_fetch_stop`), after
  `rd_kafka_assign` returns; because the application forward set `FWD_APP`,
  `rd_kafka_toppar_op_fetch_start` then leaves the queue unforwarded. `assign` now
  forwards again after an `rd_kafka_assignment` round trip, which the main thread
  serves after the stop requests (`rkcg_ops` and each `rktp_ops` forward to
  `rk_ops`). Records fetched in between wait on the partition queue and move with
  the forward (`rd_kafka_q_fwd_set0` concatenates them).
- `Kafka.Consumer.Callbacks` is re-exported wholesale through
  `Kafka.Consumer.ConsumerProperties`' `module X`, so the shared forwarding helper
  is hidden there to stay internal.
- `closeConsumer` previously destroyed the message queue but left it in the
  `IORef`; it now clears it, and `setConsumerQueueIOEvent` (EP-4) and the new
  lifecycle state both rely on that.
- An assign-only consumer against a listener that never answers connects at once,
  so the socket test needs no group activity. `closeConsumer` against such a
  listener was not exercised: it may block, which is the reason `destroyConsumer`
  does not call it.
- The deprecated `seek` creates topic handles with garbage-collector finalizers;
  they are not tracked by the lifecycle state, which is another reason operations
  after `destroyConsumer` are documented as invalid.


## Decision Log

- Decision: `pollMessage` keeps its type; attributed errors come from a new function.
  Rationale: IR-2 asks for it explicitly, and every downstream consumer pattern-matches on
  `Either KafkaError (ConsumerRecord ...)`.
  Date: 2026-10-21

- Decision: `destroyConsumer` quiesces callback polling and releases queue references, then
  runs the existing `NO_CONSUMER_CLOSE` finalizer; it does not invoke graceful close.
  Rationale: The recovered IR-2 requires bounded cleanup against a nonresponsive
  endpoint. A serialized lifecycle state makes repeated calls wait for completed
  destruction without skipping cleanup or running a second finalizer. See
  [consumer queue and teardown ownership](../adr/consumer-queue-and-teardown-ownership.md).
  Date: 2026-10-21


- Decision: `assign` does not take the callback-poll lock.
  Rationale: Forwarding before assignment keeps records off the consumer queue
  without it, and taking the lock would deadlock applications that call `assign`
  from a rebalance callback served by the background loop.
  Date: 2026-10-22

- Decision: Lifetime state is an `MVar ClientLifecycle` held for the whole close or
  destroy transition (`modifyMVar` for close, `modifyMVarMasked_` for destroy).
  Rationale: Holding it makes concurrent callers wait for completion, and its
  exception-restoring semantics let an interrupted destroy be finished by the next
  call because every step is idempotent.
  Date: 2026-10-22


## Outcomes & Retrospective

Completed 2026-10-22. An application can build a bounded, read-only observer: a
consumer from `newAssignedConsumer` reads explicitly assigned partitions in both
poll modes without the broker recording its group, `pollMessageAttributed` names the
partition and end offset of each partition EOF, and `destroyConsumer` closes the
client's connections within tens of milliseconds, safely under repetition,
concurrency and interruption. Watermarks on a `read_committed` consumer stop at the
last stable offset. IR-2's AC-1 to AC-4 are evidenced; AC-5 and the request's
completion belong to EP-6.

The integration with EP-4 is in place in the code (manual assignment forwards to the
message queue the IO event watches, and teardown clears that queue), but the combined
assigned-consumer notification and destroy check is EP-6 M1's. The inherited async
manual-assignment defect was real and only showed up with reassignment once the
obvious fix was in; the assign-only test's second offset is what caught it.


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
whose v2.15.0 implementation passes the handle's `isolation.level` through
`rd_kafka_ListOffsetsRequest` in `src/rdkafka_request.c` of
`mori://confluentinc/librdkafka` (artifact-level source URI pending): on a
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

ADR context: `docs/adr/consumer-fatal-errors-are-surfaced-in-band.md` (from EP-2)
requires every poll path to report a raised fatal in-band; `pollMessageAttributed` must
call the same fatal check as `pollMessage`. Cross-repository,
`mori://shinzui/keiro/okf/adrs/concepts/ADR-11` already states that "consumer close is
scope-sequenced rather than left to the garbage collector" from the application's side;
this plan implements and updates the local
[consumer queue and teardown ownership ADR](../adr/consumer-queue-and-teardown-ownership.md). The request's origin is
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

Document on `newConsumer` that an empty subscription skips `rd_kafka_subscribe`.
The constructor alias alone is insufficient: the imported public `assign` only calls
`rdKafkaAssign`, whereas async forwarding occurs in `Kafka.Consumer.Callbacks` only
inside rebalance callbacks. Extract shared assignment/forwarding helpers and make manual
assignment route every partition to the message queue in async mode, with balanced queue
references. The inherited `rdKafkaQueueGetPartition` already attaches a finalizer,
so use `finalizeForeignPtr` for early release rather than an additional raw destroy. Coordinate with the callback-poll lock so records cannot be discarded by the
background loop during the assign/pause/forward/resume transition; verify partition
queues are paused/forwarded before fetch results can escape to that loop. Handle errors
and reassignment explicitly. Preserve the existing sync route and subscribed-consumer
behavior. These helpers and queue ownership belong to EP-5. Add
`test-integration/Kafka/Integration/AssignOnlyTest.hs`, one case per poll mode: create a
prefixed topic with one partition; produce one record and flush; create an assigned
consumer with a fresh prefixed group id, `noAutoCommit` (offset-reset is a Subscription property, so do not combine it
with ConsumerProperties);
`assign` partition 0 at explicit `PartitionOffset 0`; poll until the record arrives (bounded
by ten seconds); then `allConsumerGroupsInfo` must not contain the group id; close the
consumer. Exercise the original empty-subscription constructor as well as the new alias,
and repeat assignment with another explicit offset to prove routing remains correct.

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
partition and offset when meaningful (negative invalid sentinels become Nothing),
then destroys the message exception-safely exactly once; a good message is converted as today.
Refactor `fromMessagePtr` to share the record-building code. In `Kafka.Consumer` add and
export `pollMessageAttributed :: MonadIO m => KafkaConsumer -> Timeout -> m (Either
ConsumerMessageError (ConsumerRecord (Maybe ByteString) (Maybe ByteString)))`, which runs
the same fatal-error check as `pollMessage` first (returning an attributed
`RdKafkaRespErrFatal` with no origin) and then polls the same queue `pollMessage` would.
Test no-origin timeout/fatal results in both modes as well as successful records.
Add `test-integration/Kafka/Integration/AttributedErrorTest.hs` in both callback modes: create a prefixed topic
with two partitions; produce one record to partition 1 and flush; create an assigned
consumer with `extraProps` setting `enable.partition.eof=true` and `noAutoCommit`;
assign both partitions at `PartitionOffsetBeginning`; poll with `pollMessageAttributed`
until two EOF errors have been seen or fifteen seconds pass, collecting them; assert
exactly one EOF names partition 0 at offset 0 and exactly one names partition 1 at
offset 1 (the end offset after one record), both carrying the topic name, and that the
record from partition 1 also arrived.

**Milestone 3: deterministic destroy.** Add serialized lifecycle state to `KafkaConf`
covering Open, Closed and Destroyed, plus any in-progress cleanup/completion state.
Update every constructor and positional pattern match in Consumer, Callbacks, Producer
and Internal.Setup. A bare test-and-set Bool is insufficient: concurrent callers must
wait for cleanup completion, and cancellation must not leave cleanup permanently skipped. In `Kafka.Consumer` add and
export:

```haskell
-- | Stop polling and destroy the underlying librdkafka client now,
-- synchronously and exactly once. Normal operations are invalid afterwards;
-- close/destroy remain idempotent. Concurrent destroys wait for cleanup.
destroyConsumer :: MonadIO m => KafkaConsumer -> m ()
```

Implementation: serialize close/destroy transitions, mask asynchronous exceptions
around ownership changes, and ensure interrupted cleanup can finish or be joined by
a subsequent caller. Disable the async loop under its callback-poll mutex and wait
until no poll is in flight; clear and release the message queue exactly once. Then
`finalizeForeignPtr` the owning client pointer and publish Destroyed only after it
returns. Do not call `closeConsumer` internally: that performs a graceful broker-facing
close and defeats the nonresponsive-endpoint requirement. Keep the existing
`NO_CONSUMER_CLOSE` finalizer as the sole client deallocator. Graceful close separately
transitions Open to Closed; close after destroy and repeated close are harmless, and
destroy after close still finalizes the client. Do not query `consumerClosed` after
finalization. Applications must serialize normal operations with teardown and must not
call teardown from a callback (which may hold the same mutex and carry a borrowed pointer). Add `test-integration/Kafka/Integration/DestroyTest.hs`:
open a listening socket on `127.0.0.1` port 0 with `Network.Socket`, accept connections on
a thread that reads and never writes; create a consumer whose `bootstrap.servers` is that
address (librdkafka connects immediately); wait for confirmed acceptance of at least one connection with a bounded poll; start
a monotonic timer immediately before `destroyConsumer`; require both its return and
`recv` EOF on all accepted connections within one second total, in both callback modes.
Bound connection establishment and cleanup so an unopened socket cannot pass vacuously.
Test repeated destroy, concurrent destroy (both await completion), close-then-destroy,
destroy-then-close, and cleanup after an interrupted teardown, without forcing GC. Update [consumer queue and teardown ownership](../adr/consumer-queue-and-teardown-ownership.md):
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
`docs/improvement-requests/2-support-a-bounded-assign-only-observer-consumer.md`: retain an open status while AC-5 is pending, add an "Implementation evidence" section naming the APIs and four test modules and noting
AC-5's tag comes from EP-6, advance the timestamp and add a dated bundle entry with `okf log add`. Append
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
`destroyConsumer` haddock and the ownership ADR state that normal operations
are invalid after it; repeated close/destroy are the documented exceptions.
The behavior is visible beyond the tests: in the socket test transcript the accepted
connection reaches end-of-file within one second of `destroyConsumer`, whereas with
`closeConsumer` alone it stays open until a garbage collection.

Required acceptance is M1 through M4. A `bracket`-style helper
(`withAssignedConsumer`) is welcome but not required; adding it is in scope only if it
costs one function.


## Idempotence and Recovery

All edits are additive; `pollMessage` and `closeConsumer` keep their types and, apart from
idempotent lifecycle guards, their behavior. Manual assignment gains correct async routing. The tests use run-prefixed names and create
their own topics, so reruns need no cleanup. If the EOF assertions are flaky because
Redpanda delivers the EOF for the empty partition before assignment settles, poll longer
rather than loosening the exact-count assertion; the count is the point of AC-2. If the
socket test cannot observe end-of-file on macOS because `recv` blocks, use a non-blocking
socket with a bounded `timeout`; the one-second bound is part of AC-3. If synchronous finalization cannot be demonstrated, stop acceptance and investigate
the ownership mechanism. Do not add an explicit C destroy alongside an independently
registered finalizer: that permits double free. Any replacement must preserve one
deallocator and revalidate callback safety before updating the ADR.


## Interfaces and Dependencies

Prerequisites: `docs/plans/2-run-integration-tests-on-a-project-local-redpanda.md`
Complete (fixtures, `just test-it`, the private broker) and
`docs/plans/3-refresh-the-librdkafka-bindings-for-2-15-and-newer.md` Complete
(binding conventions). Integration dependency: `docs/plans/4-expose-queue-event-notification-for-consumers.md`
owns event registration on the queue this plan routes. Reconcile queue lifetime,
Cabal dependencies, tests and changelog changes; EP-6 M1 verifies notifications with
manual assignment and teardown after both features are combined.

Libraries: `network` added to the integration suite for the socket test; `unix` and
`base` otherwise.

Interfaces defined here, consumed by `mori://shinzui/shoko`: `Kafka.Consumer.newAssignedConsumer`,
`Kafka.Consumer.pollMessageAttributed`, `Kafka.Consumer.Types.ConsumerMessageError(..)`,
and `Kafka.Consumer.destroyConsumer`, all with the types shown above.
