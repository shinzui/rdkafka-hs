---
id: 4
slug: expose-queue-event-notification-for-consumers
title: "Expose queue event notification for consumers"
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

# Expose queue event notification for consumers

This ExecPlan is a living document. The sections Progress, Surprises & Discoveries,
Decision Log, and Outcomes & Retrospective must be kept up to date as work proceeds.
If durable project context changes, update or create ADRs in docs/adr/ in the same change.


## Purpose / Big Picture

After this plan, an application that must not block inside `pollMessage` can wait for
records instead of polling on a timer. It registers a non-blocking file descriptor with
`Kafka.Consumer.setConsumerQueueIOEvent`, and librdkafka writes a payload to that
descriptor whenever the queue the consumer reads from goes from empty to non-empty. The
application then waits with `threadWaitRead` and polls only when woken. The
`shibuya-kafka-adapter` measurements that motivated this (idle CPU rising from 0.19% to
3.6% as the sleep between empty polls shrinks from a blocking poll to 1 ms) go away: an
idle consumer costs nothing. This closes improvement request IR-1 in
`docs/improvement-requests/1-expose-queue-event-notification-for-consumers.md`, whose
three acceptance criteria are this plan's acceptance criteria.


## Progress

- [ ] M1: `rd_kafka_queue_io_event_enable`, `rd_kafka_queue_cb_event_enable` and
      `rd_kafka_queue_get_consumer` bound; `setConsumerQueueIOEvent` enables and disables
      the IO event on the right queue in both callback poll modes; `docs/bindings.md`
      regenerated (IR-1 AC-1).
- [ ] M2: Integration test proves the pipe becomes readable before `pollMessage` returns
      the record and stays unreadable after disabling, in both poll modes (IR-1 AC-2);
      IR-1 moved to `completed` with a `log.md` entry; CHANGELOG updated. AC-3 (a tagged
      commit) is satisfied by `docs/plans/6-cut-the-first-rdkafka-hs-release-and-migration-guide.md`.


## Surprises & Discoveries

(None yet.)


## Decision Log

- Decision: Only the file-descriptor variant is exposed at the consumer level; the
  callback variant is bound but not wrapped.
  Rationale: As IR-1 explains, `rd_kafka_queue_cb_event_enable` invokes its callback on
  an internal librdkafka thread that must not call back into librdkafka for the same
  handle, which is awkward and dangerous to expose to Haskell code. Binding it satisfies
  AC-1 and keeps the door open.
  Date: 2026-10-21


## Outcomes & Retrospective

(To be filled during and after implementation.)


## Context and Orientation

`rdkafka-hs` is the Haskell binding to librdkafka. Its consumer, `Kafka.Consumer`
(`src/Kafka/Consumer.hs`), has two *callback poll modes* selected by
`ConsumerProperties`' `callbackPollMode`. In `CallbackPollModeSync` the application's own
calls to `pollMessage` drive librdkafka, and records come from the *consumer queue*
(librdkafka's `rd_kafka_queue_get_consumer`, which is not yet bound). In the default
`CallbackPollModeAsync`, `newConsumer` creates a separate *message queue* with
`rd_kafka_queue_new`, stores it in the `IORef` inside `KafkaConf`
(`src/Kafka/Internal/Setup.hs`; read it with `getRdMsgQueue`), redirects every partition's
records to it, and starts a background thread (`runConsumerLoop`) that polls the consumer
queue every 100 ms purely to serve callbacks; `pollMessage` and `pollMessageBatch` then
read the message queue. Which queue "the consumer reads from" therefore depends on the
mode, and this plan must enable the event on the right one: the message queue in async
mode, the consumer queue in sync mode. librdkafka's documentation adds two rules the
implementation must respect: with forwarded queues the event must be enabled on the final
destination queue (true of the message queue in async mode), and IO-event and
callback-event triggering are mutually exclusive on one queue.

The C API, from `librdkafka/rdkafka.h` in the pkg-config include directory (and readable
at `/Users/shinzui/Keikaku/hub/librdkafka-project/librdkafka/src/rdkafka.h`, Mori project
`mori://confluentinc/librdkafka`):

```c
void rd_kafka_queue_io_event_enable(rd_kafka_queue_t *rkqu, int fd,
                                    const void *payload, size_t size);
void rd_kafka_queue_cb_event_enable(rd_kafka_queue_t *rkqu,
                                    void (*event_cb)(rd_kafka_t *rk, void *qev_opaque),
                                    void *qev_opaque);
rd_kafka_queue_t *rd_kafka_queue_get_consumer(rd_kafka_t *rk);
```

`rd_kafka_queue_io_event_enable` with `fd = -1` disables the event. The descriptor must
be non-blocking, because librdkafka writes from an internal thread and must never block
there. `rd_kafka_queue_get_consumer` returns a reference-counted handle that must be
released with `rd_kafka_queue_destroy` (already bound as `rdKafkaQueueDestroy`); the event
setting lives on the underlying queue, so releasing the handle does not undo it. The
pointer type `RdKafkaQueueTPtr` is already declared `foreign` in
`src/Kafka/Internal/RdKafka.chs`; `rdKafkaQueueNew` attaches no finalizer and
`closeConsumer` destroys the message queue explicitly.

The binding module's conventions (from `docs/plans/3-refresh-the-librdkafka-bindings-for-2-15-and-newer.md`):
sections mirror the header's `@name` groups; plain calls are `{#fun ... as ^#}`; hand
written wrappers sit beneath their `{#fun#}`; after editing the file run
`just bindings-inventory` so `docs/bindings.md` is current, and `just bindings-check`
must pass. The queue functions belong in the "Queue API" section.

Tests: the integration suite `rdkafka-hs-integration` (tasty, flag `integration`) runs
with `just test-it` against the project-local Redpanda at `RDKAFKA_TEST_BROKER` (default
`127.0.0.1:39092`). Fixtures live in `test-integration/Kafka/TestEnv.hs` (`TestEnv`,
`withProducer`, `withConsumer`, prefixed topic and group names) and groups are registered
in `test-integration/Main.hs`, all from `docs/plans/2-run-integration-tests-on-a-project-local-redpanda.md`.
`System.Posix.IO` from the `unix` package (already a library dependency) provides
`createPipe`, `setFdOption` with `NonBlockingWrite`, and `fdRead`; `Control.Concurrent`
provides `threadWaitRead`; `System.Timeout.timeout` bounds the waits.

Downstream: once this exists, `mori://shinzui/kafka-effectful` needs an effect operation
for it and `mori://shinzui/shibuya-kafka-adapter` replaces its idle sleep
(`mori://shinzui/shibuya-kafka-adapter/okf/improvement-requests/concepts/IR-1`). Neither
is edited by this plan.

ADR context: `docs/adr/3-integration-tests-run-against-a-project-local-redpanda.md` and
`docs/adr/4-consumer-fatal-errors-are-surfaced-in-band.md` apply (tests use the private
broker; the fatal check in `pollMessage` must stay in front of any new poll path). No
cross-repository ADR constrains the design; the request originates from
`mori://shinzui/shibuya-kafka-adapter/okf/bug-reports/concepts/BUG-7`.


## Plan of Work

**Milestone 1: bindings and the consumer-level switch.** In the Queue API section of
`src/Kafka/Internal/RdKafka.chs` add `{#fun rd_kafka_queue_io_event_enable as ^
{`RdKafkaQueueTPtr', `Int', castPtr `Ptr Word8', cIntConv `CSize'} -> `()'#}`,
`{#fun rd_kafka_queue_get_consumer as ^ {`RdKafkaTPtr'} -> `RdKafkaQueueTPtr'#}` (the
handle must be released with `rdKafkaQueueDestroy` by the caller), and a binding for
`rd_kafka_queue_cb_event_enable` whose callback type is declared with a `FunPtr`
(`foreign import ccall "wrapper"` is not needed since no Haskell wrapper is exposed; the
`{#fun#}` takes a `FunPtr` argument). In `src/Kafka/Consumer.hs` add and export:

```haskell
-- | Ask librdkafka to write the payload to the descriptor whenever the queue
-- that 'pollMessage' and 'pollMessageBatch' read from goes from empty to
-- non-empty. The descriptor must be non-blocking. 'Nothing' disables it.
setConsumerQueueIOEvent :: MonadIO m => KafkaConsumer -> Maybe (Fd, ByteString) -> m ()
```

Its implementation picks the queue from `getRdMsgQueue`: `Just q` means async mode, so
enable on `q`; `Nothing` means sync mode, so obtain the consumer queue with
`rdKafkaQueueGetConsumer`, enable on it, and release it with `rdKafkaQueueDestroy` in a
`bracket`. For `Just (fd, payload)` use `unsafeUseAsCStringLen` on the payload and the
numeric fd; for `Nothing` pass fd `-1`, a null payload and size zero. The haddock must
state the three librdkafka rules (non-blocking descriptor; final destination queue; IO and
callback variants mutually exclusive) and that the setting persists until disabled or the
consumer is closed. Run `just bindings-inventory` and commit `docs/bindings.md`.

**Milestone 2: the test and the request.** Add
`test-integration/Kafka/Integration/QueueEventTest.hs` with a group run once per poll
mode. Each case: create a prefixed topic; create a consumer subscribed to it with the
fixture that waits for assignment; drain any initial messages with a short poll; create a
pipe with `createPipe`, set the write end non-blocking with `setFdOption wfd
NonBlockingWrite True`; call `setConsumerQueueIOEvent consumer (Just (wfd, "1"))`;
produce one record and flush; `timeout 10_000_000 (threadWaitRead rfd)` must return
`Just ()`, then `fdRead rfd 1` returns `"1"`, and only then `pollMessage consumer
(Timeout 1000)` must return `Right` the record (in sync mode the record may already have
been fetched by librdkafka's threads before the poll, which is fine; the assertion is that
the pipe became readable before the application polled). Then
`setConsumerQueueIOEvent consumer Nothing`, produce a second record and flush, and
`timeout 2_000_000 (threadWaitRead rfd)` must return `Nothing`; finally `pollMessage`
still returns the second record, proving disabling the event did not break delivery.
Close the descriptors. Register the group in `test-integration/Main.hs`. Then edit
`docs/improvement-requests/1-expose-queue-event-notification-for-consumers.md`: set
`status: completed`, add a short "Resolution" section naming
`Kafka.Consumer.setConsumerQueueIOEvent` and the test module, note that AC-3's tag is cut
by EP-6, and add a dated entry to `docs/improvement-requests/log.md`. Append the
CHANGELOG entry under `Unreleased` / `Added`.


## Concrete Steps

All commands run from `/Users/shinzui/Keikaku/bokuno/rdkafka-hs` inside the dev shell.

```bash
grep -n "rd_kafka_queue_io_event_enable\|rd_kafka_queue_get_consumer" \
  "$(pkg-config --variable=includedir rdkafka)/librdkafka/rdkafka.h"
```

Both names must appear. After Milestone 1:

```bash
cabal build rdkafka-hs
just bindings-inventory && just bindings-check
grep -n "queue_io_event_enable\|queue_cb_event_enable\|queue_get_consumer" docs/bindings.md
```

All three must be listed as bound. After Milestone 2:

```bash
just redpanda-up
cabal test rdkafka-hs-integration -f integration --test-options='-p "/queue event/"'
just test-it
okf validate docs/improvement-requests --strict \
  --profile docs/improvement-requests/profile.dhall --profile-enforce --log-enforce
```

The focused run prints two passing cases, `async` and `sync`. Every commit carries:

```text
MasterPlan: docs/masterplans/1-fork-hw-kafka-client-as-rdkafka-hs.md
ExecPlan: docs/plans/4-expose-queue-event-notification-for-consumers.md
Intention: intention_01m5gc388wecfsq431s3pgg1dd
```


## Validation and Acceptance

Accepted when IR-1's AC-1 and AC-2 hold as written in the request file: the module exports
both event bindings and the library builds against librdkafka 2.15; the test creates a
non-blocking pipe, enables the event, produces one record to an idle consumer, observes the
read end become readable before `pollMessage` returns that record, and after disabling the
event a further record leaves the pipe unreadable, in both callback poll modes. `just
test-it` as a whole still passes, `just bindings-check` passes, `nix build` passes, and
`okf validate` accepts the updated improvement-request bundle. AC-3 is accepted when EP-6
tags the release that contains this commit.


## Idempotence and Recovery

Enabling the event twice replaces the previous setting; disabling an event that was never
enabled is harmless. The test closes its pipe descriptors in a `finally` so a failed
assertion does not leak them into later cases. If `threadWaitRead` never wakes in async
mode, the most likely cause is enabling the event on the consumer queue instead of the
message queue; check which branch `getRdMsgQueue` took. If it never wakes in sync mode,
confirm the consumer queue handle was obtained with `rd_kafka_queue_get_consumer` after
`rd_kafka_poll_set_consumer` ran (it runs inside `newConsumer`).


## Interfaces and Dependencies

Prerequisites: `docs/plans/2-run-integration-tests-on-a-project-local-redpanda.md`
Complete (fixtures and `just test-it`) and `docs/plans/3-refresh-the-librdkafka-bindings-for-2-15-and-newer.md`
Complete (binding conventions, `just bindings-check`). Soft: `docs/plans/5-support-a-bounded-assign-only-observer-consumer.md`
edits the same `Kafka.Consumer` export list and `test-integration/Main.hs`; whichever
lands second rebases.

Libraries: `unix` (`System.Posix.IO`, `System.Posix.Types.Fd`), `bytestring`
(`Data.ByteString.Unsafe.unsafeUseAsCStringLen`), `base` (`threadWaitRead`,
`System.Timeout`).

Interface defined here: `Kafka.Consumer.setConsumerQueueIOEvent :: MonadIO m =>
KafkaConsumer -> Maybe (Fd, ByteString) -> m ()`, consumed later by
`mori://shinzui/kafka-effectful` and `mori://shinzui/shibuya-kafka-adapter`.
