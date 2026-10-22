---
type: Architecture Decision Record
title: Consumer fatal errors are surfaced in-band
description: Once librdkafka raises a fatal error on a consumer, pollMessage and pollMessageBatch return KafkaResponseError RdKafkaRespErrFatal on every call in both callback poll modes, consumerFatalError returns the cause, and the background loop destroys what it polls.
timestamp: 2026-10-22T00:36:35Z
generated:
  by: claude-code/claude-opus-5-5
  at: "2026-10-22T00:36:35Z"
docId: ADR-6
status: Accepted
date: 2026-10-21
originatingPlan: docs/plans/2-run-integration-tests-on-a-project-local-redpanda.md
---

# Consumer fatal errors are surfaced in-band

## Context

librdkafka delivers a fatal error raised on a high-level consumer (for example a
static group member fenced by another consumer with the same `group.instance.id`)
as a consumer error on the consumer queue, and never through `error_cb`. In
`CallbackPollModeAsync` the application drains a separate queue while the
binding's background callback-poll loop drains the consumer queue, and that loop
discarded what it polled without destroying it. A fatal error was therefore
invisible in async mode, and every message the loop polled leaked.
`mori://shinzui/keiro/okf/adrs/concepts/ADR-11` records the consumer-side contract
this library must provide and that the fix can only live in the binding.

## Decision

Before polling, `pollMessage` and `pollMessageBatch` read librdkafka's fatal state
with `rd_kafka_fatal_error`. Once a fatal error has been raised, every call returns
`Left (KafkaResponseError RdKafkaRespErrFatal)` (a singleton batch for
`pollMessageBatch`) without polling, in both callback poll modes, until the consumer
is closed. The generic code is used in both modes because it is what librdkafka
itself hands a sync-mode application. `consumerFatalError` returns the underlying
cause and librdkafka's description, for example
`RdKafkaRespErrFencedInstanceId`.

The background callback-poll loop destroys every message its poll returns.

The integration suite's `fatal` group is the regression test: it fences a static
member in each poll mode and asserts the in-band error, its persistence, the
recovered cause and, in async mode, the batch result. It fails when the in-band
check is removed.

## Consequences

Applications classify a fatal consumer by matching `RdKafkaRespErrFatal` from any
poll, independent of poll mode, and recreate the consumer. In async mode the
background loop serves callbacks, so `max.poll.interval.ms` cannot detect an
application that has stopped polling its own queue; applications that need that
protection use `CallbackPollModeSync`. Later consumer features must preserve the
pre-poll fatal check.
