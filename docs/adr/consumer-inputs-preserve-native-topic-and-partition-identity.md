---
type: Architecture Decision Record
title: Consumer inputs preserve native topic and partition identity
description: Reject unrepresentable topic, group, partition and SASL credential arguments before native conversion or state changes.
timestamp: 2026-10-25T01:04:47Z
generated:
  by: codex-cli/gpt-6.1-sol
  at: "2026-10-25T01:04:47Z"
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

SASL credential replacement uses the same identity rule for both client kinds:
reject NUL in either username or password with KafkaBadSpecification before
calling the native setter. A reproduced regression shows the previous setter
reported success while C silently truncated both kinds of input. Representable
credentials still change only future authentications, as specified by the native
API; real reconnect/ACL qualification is owned by EP-19. Mask the native error
acquisition through installation of decoding cleanup. The cancellation control
reproduced one allocated but undestroyed real native error before that repair.

## Consequences

A caller cannot accidentally target another topic or partition through truncation or wrapping. Regression tests require the preexisting assignment and next record to survive rejection in both callback modes. Metadata convenience functions inherit the same checks through their underlying queries. Actual authentication, TLS, authorization and reconnect behavior remain EP-19 obligations; plaintext ownership tests alone cannot close them.

Native source is `mori://confluentinc/librdkafka`, project-relative `src/rdkafka.h` and `src/rdkafka_partition.c`; source-artifact URI coverage is pending. The local bindings marshal partition IDs through native C integers and topic/group identities through C strings.
