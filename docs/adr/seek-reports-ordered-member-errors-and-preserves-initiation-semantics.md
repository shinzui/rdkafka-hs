---
type: Architecture Decision Record
title: Seek reports ordered member errors and preserves initiation semantics
description: Preserve the public Maybe KafkaError result while reporting native overall failures first and member failures in request order, with pending markers normalized only for zero timeout.
timestamp: 2026-10-24T10:55:10Z
generated:
  by: codex-cli/gpt-6.1-sol
  at: "2026-10-24T10:55:10Z"
docId: ADR-14
status: Accepted
date: 2026-10-24
originatingPlan: docs/plans/10-report-per-partition-seek-failures.md
---

# Seek reports ordered member errors and preserves initiation semantics

## Context

Librdkafka's seek call can return overall success while writing failures into individual partition entries. The inherited wrapper discarded those fields, concealing both failed seeks and failed rewinds. Public `TopicPartition` intentionally carries an offset rather than a member error, and changing its shape would affect unrelated list conversions.

## Decision

Keep `seekPartitions` and `rewindConsumer` returning `Maybe KafkaError`. An overall native error takes precedence. Otherwise read member errors while retaining the native list and select the first substantive error in request order. The generic public partition conversion remains unchanged.

Only a zero timeout normalizes `IN_PROGRESS`: such a call promises initiation without waiting for completion. Immediately available substantive failures still propagate. A nonzero timeout cannot turn residual pending markers into completed success. A mixed request can partially succeed; returning an error does not undo the successful members.

Rewind uses the committed next offset unchanged, falls back to beginning only when no committed offset exists, and propagates assignment, lookup and seek failures. It does not make the lookup and seek atomic with concurrent reassignment. Applications serialize these operations when they need a stable assignment.

## Consequences

Tests in [EP-10](../plans/10-report-per-partition-seek-failures.md) compare native error codes and exact replayed offsets/payloads in both callback modes. Native boundary seams cover overall precedence, pending markers and a real assignment change after lookup. The supported native implementation updates only the first matching duplicate entry on reply, so a synchronous duplicate request can retain `IN_PROGRESS`; callers must not interpret that as completed success.

Logical offsets follow the native seek contract: beginning/end and tail positions are supported, `STORED` is rejected by synchronous seek, and invalid or out-of-range positions follow the configured reset policy. A zero-timeout call cannot report a rejection that occurs later on the native thread. The source of these semantics is `mori://confluentinc/librdkafka` with project-relative `librdkafka/src/rdkafka.h`, `librdkafka/src/rdkafka.c` and `librdkafka/src/rdkafka_partition.c` (source artifact URIs pending).
