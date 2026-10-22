---
type: Improvement Request
title: Expose librdkafka's queue event notification for consumers
description: Bind rd_kafka_queue_io_event_enable and apply it to the queue the consumer reads from, so an application can wait for records without blocking in a poll or polling on a timer.
generated:
  by: claude-code/claude-fable-5-1
  at: "2026-09-30T17:52:37Z"
requestId: IR-1
status: proposed
origin: mori://shinzui/shibuya-kafka-adapter/okf/bug-reports/concepts/BUG-7
acceptanceCriteria:
  - id: AC-1
    statement: Kafka.Internal.RdKafka binds rd_kafka_queue_io_event_enable and rd_kafka_queue_cb_event_enable with the argument shapes of the C API.
    verification: The module exports both bindings and the library builds against the librdkafka version the cohort pins.
  - id: AC-2
    statement: A consumer-level function enables the IO event on whichever queue pollMessage and pollMessageBatch read from, in both callback poll modes, and can disable it again.
    verification: A test creates a non-blocking pipe, enables the event, produces one record to an idle consumer, and observes the read end become readable before pollMessage returns that record; disabling the event and producing again leaves the pipe unreadable.
  - id: AC-3
    statement: A release or pinned commit of the fork carries the change so downstream packages can depend on it.
    verification: The commit is tagged or pinned and named in the fork's changelog.
reviews:
  - kind: model
    reviewer: claude-code/claude-fable-5-1
    reviewed_at: "2026-09-30T17:52:37Z"
    document_timestamp: "2026-09-30T17:52:37Z"
    scope: technical-accuracy
    outcome: commented
    provider: Anthropic
    model: claude-fable-5-1
    effort: medium
    context: Author's own check against the librdkafka 2.x header text for both functions, the hw-kafka-client 5.3.0 source from Hackage, and idle-CPU measurements taken in shibuya-kafka-adapter. Not an independent review.
---

# Expose librdkafka's queue event notification for consumers

## What is missing

librdkafka can tell an application when a queue goes from empty to non-empty:

- `rd_kafka_queue_io_event_enable(rkqu, fd, payload, size)` writes `payload` to a
  file descriptor;
- `rd_kafka_queue_cb_event_enable(rkqu, event_cb, opaque)` calls a callback from an
  internal librdkafka thread.

hw-kafka-client 5.3.0 binds neither. In `CallbackPollModeAsync` the queue that
`pollMessage` and `pollMessageBatch` read from is also internal
(`kcfgMessagesQueue`), so an application cannot call the C functions itself without
reaching into library internals.

## Why it matters

An application that must not block inside `pollMessage` has no way to wait for
records. It has to poll with a zero timeout and sleep between attempts, which trades
latency against idle CPU.

The case that raised this request is `shibuya-kafka-adapter`, which serializes every
call on a consumer behind its own lock because one thread polls and another stores
offsets and seeks. Blocking in a poll while holding that lock stalls the other thread
(`mori://shinzui/shibuya-kafka-adapter/okf/bug-reports/concepts/BUG-7`), so the
polling thread drains the queue with `pollMessage (Timeout 0)` and sleeps when the
queue is empty. Measured on an idle consumer (macOS aarch64, GHC 9.12.4, threaded
runtime):

| Wait between empty polls | Wake-ups per second | Process CPU (share of one core) |
|---|---|---|
| Blocking batch poll, for comparison | ~3 | 0.19% |
| 50 ms | ~20 | 0.31% |
| 10 ms | ~100 | 0.90% |
| 1 ms | ~1000 | 3.6% |

With the IO event, the application could register a non-blocking pipe and wait on its
read end with `threadWaitRead`, waking when a record arrives and not otherwise.

## Proposed shape

1. Bind both functions in `Kafka.Internal.RdKafka`.
2. Add a consumer-level function that applies the IO event to whichever queue
   `pollMessage` and `pollMessageBatch` read from: the library's message queue in
   `CallbackPollModeAsync`, and the consumer queue (`rd_kafka_queue_get_consumer`) in
   `CallbackPollModeSync`. For example:

   ```haskell
   -- | Ask librdkafka to write the payload to the descriptor whenever the queue
   -- that 'pollMessage' and 'pollMessageBatch' read from goes from empty to
   -- non-empty. The descriptor must be non-blocking. 'Nothing' disables it.
   setConsumerQueueIOEvent :: MonadIO m => KafkaConsumer -> Maybe (Fd, ByteString) -> m ()
   ```

Points from the librdkafka documentation that the binding must respect:

- with forwarded queues the event must be enabled on the final destination queue,
  which in async mode is the library's message queue;
- the descriptor must be non-blocking;
- IO and callback triggering are mutually exclusive on one queue.

The descriptor variant should be the primary API. The callback variant runs on an
internal librdkafka thread and must not call librdkafka for the same handle, which is
awkward for a Haskell callback.

## Upstream status

Not filed upstream. `haskell-works/hw-kafka-client` last pushed in October 2025 and
its issue #213, which asked in July 2026 whether maintained improvements would be
accepted, has no reply. This fork will carry the change.

## Who consumes it

Once the binding exists, `mori://shinzui/kafka-effectful` needs an effect operation
that exposes it, and `shibuya-kafka-adapter` then replaces its idle sleep with a wait
on the descriptor; that last step is
`mori://shinzui/shibuya-kafka-adapter/okf/improvement-requests/concepts/IR-1`.
