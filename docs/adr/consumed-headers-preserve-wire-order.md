---
type: Architecture Decision Record
title: Consumed headers preserve wire order
description: Headers is an ordered list that keeps every pair, repeated names and identical pairs included, and the consumer decoder returns it in the order the producer sent it; selecting a value for a repeated name is the caller's choice.
timestamp: 2026-10-23T23:20:29Z
generated:
  by: claude-code/claude-opus-5-5
  at: "2026-10-23T23:20:29Z"
docId: ADR-9
status: Accepted
date: 2026-10-23
originatingPlan: docs/plans/7-preserve-wire-order-when-decoding-record-headers.md
---

# Consumed headers preserve wire order

## Context

Kafka record headers are `(name, value)` byte-string pairs, and a name may
repeat. Downstream consumers such as Shoko select the last occurrence of a
repeated name for event type and business-key correlation, so the order of the
decoded list is part of the public contract, not an implementation detail.

rdkafka-hs 1.0.0.0 decoded headers in reverse: `readHeaders` in
`src/Kafka/Internal/Shared.hs` read indices from zero upward with
`rd_kafka_header_get_all` but prepended each pair and returned the accumulator
unchanged. The producer (`withHeaders` in `src/Kafka/Producer.hs`) already sent
the list in order. The old header test compared sets, so the reversal passed.
See [IR-3](../improvement-requests/3-preserve-wire-order-when-decoding-record-headers.md).

## Decision

`Headers` stays a wrapper around `[(ByteString, ByteString)]`. `headersFromList`
sends the pairs in list order, and every consumer path decodes them through the
shared `readHeaders` in wire order, keeping repeated names and identical pairs.
The decoder prepends and reverses once at the native end-of-list result, which
stays linear in the number of headers.

The library does not sort, deduplicate, or choose a value for a repeated name.
It exports no selector; callers fold the list with whichever policy they need.

Header order is covered by broker regressions in
`test-integration/Kafka/Integration/HeadersTest.hs` that compare complete lists
through `pollMessage` and `pollMessageAttributed` in both callback poll modes,
for interleaved duplicates, distinct names, identical repeated pairs, one header
and none. Every multi-pair fixture is asymmetric, so a reversal cannot pass.

## Consequences

The fix shipped as the patch release 1.0.0.1 with unchanged signatures. Any
future change to header decoding, such as a nullable-value representation or a
different error policy, must keep the sequence and duplicate guarantees and the
order-sensitive regressions. Order-insensitive comparisons (sets, sorting) are
not acceptable evidence for header behaviour.
