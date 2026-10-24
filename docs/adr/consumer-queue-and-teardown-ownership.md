---
type: Architecture Decision Record
title: Consumer queue and teardown ownership
description: Manual assignment shares subscription queue routing, and destroyConsumer synchronously stops polling, releases queue references and destroys the client without a graceful group close.
timestamp: 2026-10-24T22:12:14Z
generated:
  by: codex-cli/gpt-6-astra
  at: "2026-10-22T00:05:10Z"
docId: ADR-1
status: Accepted
date: 2026-10-21
---

# Consumer queue and teardown ownership

## Context

The imported consumer routes records to a separate queue in async callback mode.
Its rebalance callback forwards partition queues, but manual `assign` does not.
The background callback loop can therefore consume and discard manually assigned
records. A consumer also owns a client finalizer and a message-queue reference;
explicit destruction must coordinate these resources with that loop.

## Decision

Manual assignment and subscription rebalances must share queue-routing behavior.
EP-5 owns that behavior and consumer lifetime state; EP-4 registers notifications
on the resulting destination queue. Queue acquisition, forwarding and release
must have explicit ownership, with no record-loss interval during assignment.

`closeConsumer` remains the graceful group-close operation. `destroyConsumer`
stops callback polling, releases owned queue references and synchronously runs the
existing client finalizer with `NO_CONSUMER_CLOSE`. It does not first perform a
potentially blocking graceful group close. Callers wanting graceful departure
close before destroying. Concurrent destroy calls join the same cleanup and return
only after it finishes; cleanup is not skipped because a flag was set early.

Applications serialize other operations with teardown and do not destroy from a
librdkafka callback. Event descriptors must stay open until notification has been
disabled or the client has been destroyed. A queue notification indicates a queue
transition, not a promise of a record or notification of every fatal state; async
applications retain bounded fatal-state checks when waiting for records.

## Implementation

Implemented on 2026-10-22 by
[the assign-only observer plan](../plans/5-support-a-bounded-assign-only-observer-consumer.md).

Queue routing lives in `Kafka.Consumer.Callbacks.forwardPartitionsToMessageQueue`,
used by both the rebalance callback and `Kafka.Consumer.assign`, and it releases
each partition-queue reference as soon as the forward is set. Manual assignment
forwards before `rd_kafka_assign`, so no record can reach librdkafka's consumer
queue (which the async background loop drains and discards). It forwards again
after an `rd_kafka_assignment` round trip: reassigning a partition stops its
fetcher, and librdkafka clears the partition queue's forwarding when that stop is
processed on its main thread, after `rd_kafka_assign` has returned. Because the
stop requests and the `rd_kafka_assignment` request are served in order by that
thread, the second forward follows the stop; records fetched in between wait on
the partition queue, which librdkafka does not re-forward once the application
has forwarded it, and move with the forward.

Lifetime state is a `ClientLifecycle` (`ClientOpen`, `ClientClosed`,
`ClientDestroyed`) in an `MVar` on the internal `KafkaConf`. `closeConsumer`
moves an open consumer to closed and is otherwise a no-op returning `Nothing`; it
now also clears the message-queue reference it releases. `destroyConsumer`
holds the `MVar` for the whole teardown with asynchronous exceptions masked:
it disables callback polling (waiting for an in-flight poll), clears and
releases the message queue, and runs `finalizeForeignPtr` on the client, whose
`NO_CONSUMER_CLOSE` finalizer remains the only deallocator. Concurrent callers
block on the `MVar` until the teardown is done. An interrupted teardown leaves
the previous state in place, and every step is safe to repeat, so the next call
completes it. The garbage-collector finalizer is now only a safety net for
consumers that are never destroyed.

A second, explicit `rd_kafka_destroy` path was rejected: alongside the
registered finalizer it would allow a double free, and finalizing the existing
foreign pointer keeps a single deallocator with the same callback-safe concurrent
finalizer. `setConsumerQueueIOEvent` does nothing once polling is disabled, so a
notification cannot be registered on a released queue.

Managed input partition lists have a two-stage ownership handoff. The converter
owns the list while adding and encoding members; `bracketOnError` finalizes a
partial list if conversion throws. Only a successfully returned list transfers
to the public operation's bracket. Relying on the outer bracket alone leaves
failed acquisition cleanup deferred to GC. Assignment and subscription output
lists remain owned through decoding and cancellation. Native-counted controls
check release before GC, including a real foreign-encoding failure after an
ASCII member, invalid output bytes, and held native return boundaries.

## Consequences

Both callback modes need manual-assignment and teardown regression tests. EP-6
must exercise queue notification on an assigned consumer after EP-4 and EP-5 are
combined. The socket test measures elapsed time from entering destroy through
observing EOF, without forcing garbage collection. Graceful close and immediate
destroy remain distinct public operations.

This decision preserves the fatal-error contract in
`mori://shinzui/keiro/okf/adrs/concepts/ADR-11` and implements the request in
`mori://shinzui/hw-kafka-client/okf/improvement-requests/concepts/IR-2`.
EP-1 will adopt this existing record into the profiled ADR bundle and allocate
its stable handle using OKF.

Persistent Haskell callback bridges now follow [ADR-15](native-clients-own-persistent-haskell-callback-bridges.md). The native client finalizer retains their independent owner through shutdown and releases it afterward. Failed construction releases unattached registrations and destroys a native configuration duplicate left with the caller. Public operations scope native child handles and retain the client through their cleanup, rather than relying on GC finalizer ordering. The lifecycle lock still publishes `ClientDestroyed` only after synchronous finalization completes.

A sync consumer-queue reference is a temporary native wrapper. Destroying that
reference frees the wrapper even while the underlying queue remains owned by
the client. Observers inspect the wrapper only within its borrowing call and
retain scalar observations, or acquire a separately owned reference; retaining
the client alone does not extend a released wrapper's lifetime. The native
contract is in `mori://confluentinc/librdkafka`, project-relative
`src/rdkafka_queue.c`; the source-artifact URI is pending.

Notification fixtures keep pipes outside the consumer's teardown bracket, so
both normal and failing actions destroy the client before closing descriptors.
They observe a queued record before enabling preloaded notifications, preserve
unexpected producer/poll/descriptor errors, and check descriptor ownership after
both graceful close and immediate destruction.
