---
type: Architecture Decision Record
title: Consumer inputs preserve native topic and partition identity
description: Reject unrepresentable topic, group and partition arguments before native conversion or state changes.
timestamp: 2026-10-24T21:23:16Z
generated:
  by: codex-cli/gpt-6.1-sol
  at: "2026-10-24T21:23:16Z"
docId: ADR-26
status: Accepted
date: 2026-10-24
originatingPlan: docs/plans/14-cover-the-public-api-and-pure-data-laws.md
---

# Consumer inputs preserve native topic and partition identity

## Context

The consumer and metadata APIs accept Haskell topic text and Int partition IDs. C strings cannot represent embedded NUL, and native partition fields cannot represent integers outside the C int range. A bounded broker probe demonstrates assignment of a NUL-suffixed name selecting its prefix and partition 4294967296 selecting partition zero, including real record delivery from those unintended identities. All 78 compiled per-operation regressions fail against the inherited conversion behavior.

## Decision

Reject embedded NUL in consumer topic and group query arguments, and reject partition integers outside the native C int range. Return KafkaBadSpecification through each operation's existing result shape before allocating native argument lists or changing assignment, pause, storage, commit or seek state. Reject an entire malformed input list before any member is submitted; native errors on representable inputs retain their existing partial-success semantics.

Apply the rule to subscription construction, manual assignment, pause/resume, store/commit, offset queries, both seek entry points and metadata queries. Share the representation checks internally. Keep public signatures, native validation of other malformed inputs and representable negative partition conventions unchanged. Configuration setters retain their separate exception policy in [ADR-24](configuration-strings-reject-embedded-nul.md).

## Consequences

A caller cannot accidentally target another topic or partition through truncation or wrapping. Regression tests require the preexisting assignment and next record to survive rejection in both callback modes. Metadata convenience functions inherit the same checks through their underlying queries. SASL authentication and credential replacement remain EP-15 obligations.

Native source is `mori://confluentinc/librdkafka`, project-relative `src/rdkafka.h` and `src/rdkafka_partition.c`; source-artifact URI coverage is pending. The local bindings marshal partition IDs through native C integers and topic/group identities through C strings.
