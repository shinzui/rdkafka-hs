---
type: Improvement Request
title: Support a bounded, assign-only observer consumer
description: Let a consumer be built without subscribing, report which partition a partition-EOF or per-message error belongs to, and destroy the client deterministically, so a read-only observer can scan partitions to their end without joining a group or leaking a client.
generated:
  by: claude-code/claude-opus-5-5
  at: "2026-10-21T22:08:35Z"
requestId: IR-2
status: proposed
origin: mori://shinzui/shoko/plans/10-implement-the-kafka-topic-evidence-adapter
acceptanceCriteria:
  - id: AC-1
    statement: newConsumer given an empty topic set creates a usable consumer without calling rd_kafka_subscribe, in both callback poll modes, and assign with explicit partition offsets then delivers records.
    verification: A test against a real broker builds a consumer with a fresh group.id and an empty Subscription, assigns one partition at an explicit offset, polls a record produced to it, and afterwards kafka-consumer-groups.sh --list does not show that group.id.
  - id: AC-2
    statement: A public polling function returns per-message errors, including RD_KAFKA_RESP_ERR__PARTITION_EOF, together with the topic, partition, and offset librdkafka attached to the errored message; pollMessage keeps its current type.
    verification: A test assigns two partitions of one topic, one empty and one holding a record, with enable.partition.eof=true, and receives exactly one EOF value naming each partition, with the busy partition's EOF offset equal to its end offset.
  - id: AC-3
    statement: A public function destroys a consumer's rd_kafka_t synchronously, exactly once, without forced garbage collection, so the registered finalizer becomes a no-op and sockets are closed when the call returns.
    verification: A test points bootstrap.servers at a loopback socket that accepts and never answers, polls briefly, calls the destroy function, and observes the accepted connection reach end-of-file within one second; calling the destroy function twice is safe.
  - id: AC-4
    statement: The released partitionWatermarkOffsets on a consumer configured with isolation.level=read_committed reports the last stable offset as the high offset while a transaction is open.
    verification: A test opens a transaction holding one record, queries the watermark from a read_committed consumer and from a read_uncommitted one, sees the read_committed high offset stop before the open transaction, and sees it advance after commit.
  - id: AC-5
    statement: A tagged commit of the fork carries AC-1 through AC-4 so downstream packages can pin it immutably.
    verification: The tag exists on https://github.com/shinzui/hw-kafka-client and its commit builds against librdkafka 2.15.0 with GHC 9.12.
reviews:
  - kind: model
    reviewer: claude-code/claude-opus-5-5
    reviewed_at: "2026-10-21T22:08:35Z"
    document_timestamp: "2026-10-21T22:08:35Z"
    scope: technical-accuracy
    outcome: commented
    provider: Anthropic
    model: claude-opus-5-5
    effort: medium
    context: Author's own check against the hw-kafka-client 5.3.0 Hackage tarball, this fork's branch fix/async-consumer-fatal-observability (6caed63), and librdkafka's public header. Not an independent review; no broker run.
---

# Support a bounded, assign-only observer consumer

## What is missing

Shōko needs a Kafka *observer*: a consumer that reads chosen partitions of one
topic from a known offset to the partition's end, never joins a consumer group,
never commits offsets, and is gone (sockets closed) when the read's deadline
expires. hw-kafka-client 5.3.0 cannot build one through its public API:

1. **Construction always subscribes.** In 5.3.0, `newConsumer` calls `subscribe`
   unconditionally. librdkafka rejects an empty subscription with
   `RD_KAFKA_RESP_ERR__INVALID_ARG`, so `newConsumer props (topics [])` fails, and
   subscribing to any real topic joins a group. Upstream commit
   `56be4a6f61f17f91eeb1c7d0feac4084bd10ce93` ("Don't call subscribe when there's no
   topics to subscribe to.") fixes this and is in this fork's history, but no
   release carries it and no test covers the assign-only path.
2. **Per-message errors lose their origin.** `Kafka.Consumer.Convert.fromMessagePtr`
   turns an errored message into `KafkaResponseError err`, dropping the
   `rkt`/`partition`/`offset` fields librdkafka sets on it. With
   `enable.partition.eof=true`, an observer assigned several partitions receives
   anonymous `RdKafkaRespErrPartitionEof` values and cannot tell which partition
   finished, so it cannot prove every partition was read to its end. The workaround
   of one consumer per partition multiplies connections and still cannot attribute
   other per-message errors.
3. **No deterministic destroy.** `closeConsumer` calls `rd_kafka_consumer_close`
   only. `rd_kafka_destroy_flags(rk, RD_KAFKA_DESTROY_F_NO_CONSUMER_CLOSE)` runs from
   a concurrent `ForeignPtr` finalizer registered in
   `Kafka.Internal.RdKafka.newRdKafkaT`, that is, whenever the garbage collector gets
   to it. A caller with a deadline cannot know when the client's broker threads and
   sockets are released, and `Kafka.Internal.*` is hidden, so it cannot run the
   finalizer itself.

## Why it matters

Shōko verifies distributed behavior from bounded evidence. Its Kafka topic reader
(`mori://shinzui/shoko/plans/10-implement-the-kafka-topic-evidence-adapter`) may only
claim complete history when every baseline partition was read to its end. It must
not create consumer-group state in an environment it is observing, and it must
release a stalled connection within a request deadline of tens of milliseconds.
Gaps 1 to 3 block that adapter's first milestone; the plan records them as its
blocker and points here.

## Proposed shape

1. **Assign-only construction.** Keep the `56be4a6` behavior (skip `subscribe` for
   an empty topic set) and add a test for it. Optionally expose the intent directly,
   for example `newAssignedConsumer :: MonadIO m => ConsumerProperties -> m (Either
   KafkaError KafkaConsumer)`, documented as never subscribing.
2. **Attributed errors.** Add a polling function that keeps the origin, leaving
   `pollMessage` unchanged:

   ```haskell
   data ConsumerMessageError = ConsumerMessageError
     { cmeError     :: !KafkaError
     , cmeTopic     :: !(Maybe TopicName)
     , cmePartition :: !(Maybe PartitionId)
     , cmeOffset    :: !(Maybe Offset)
     }

   pollMessageAttributed
     :: MonadIO m
     => KafkaConsumer
     -> Timeout
     -> m (Either ConsumerMessageError (ConsumerRecord (Maybe ByteString) (Maybe ByteString)))
   ```

   A timeout still has no origin (`rd_kafka_consumer_poll` returns `NULL`), so the
   origin fields are `Maybe`.
3. **Deterministic destroy.** Add `destroyConsumer :: MonadIO m => KafkaConsumer ->
   m ()` that disables the callback loop (as `closeConsumer` does through the status
   `MVar`), destroys any background queue, and then calls
   `Foreign.ForeignPtr.finalizeForeignPtr` on the `rd_kafka_t` pointer. That runs the
   existing `rd_kafka_destroy_flags(.., NO_CONSUMER_CLOSE)` finalizer immediately and
   exactly once, without a second destroy path. Document that every other operation
   on the consumer is invalid afterwards. A `bracket`-style helper is welcome but
   optional.
4. **Read-committed watermarks.** No code change is expected:
   `partitionWatermarkOffsets` calls `rd_kafka_query_watermark_offsets`, which
   sends the handle's configured isolation level. AC-4 only pins that behavior with
   a test, because Shōko depends on it to start reading at the committed-visible
   boundary.

## Upstream status

Not filed upstream yet. Gap 1 is already fixed on upstream `main` but unreleased
(Hackage's newest version is still 5.3.0, uploaded 2023-08-05). This fork will carry
the change first; it can be offered upstream afterwards.

## Who consumes it

`mori://shinzui/shoko` pins the tagged commit with a `source-repository-package`
stanza, the convention `mori://shinzui/kafka-effectful` already uses for this fork.
It builds `Shoko.Adapter.Kafka`'s scanner on `newConsumer` with an empty
subscription (or `newAssignedConsumer`), `assign`, `pollMessageAttributed`,
`partitionWatermarkOffsets`, and `destroyConsumer` inside `bracket`.
