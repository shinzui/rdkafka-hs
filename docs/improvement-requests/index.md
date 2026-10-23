---
okf_version: "0.2"
---

# Files

- [profile.dhall](profile.dhall)

# Improvement Request

- [Expose librdkafka's queue event notification for consumers](1-expose-queue-event-notification-for-consumers.md) - Bind rd_kafka_queue_io_event_enable and apply it to the queue the consumer reads from, so an application can wait for records without blocking in a poll or polling on a timer.
- [Support a bounded, assign-only observer consumer](2-support-a-bounded-assign-only-observer-consumer.md) - Let a consumer be built without subscribing, report which partition a partition-EOF or per-message error belongs to, and destroy the client deterministically, so a read-only observer can scan partitions to their end without joining a group or leaking a client.


- [Preserve wire order when decoding record headers](3-preserve-wire-order-when-decoding-record-headers.md) - Preserve ordered duplicate headers so last-value event-type and correlation selection stays correct.
