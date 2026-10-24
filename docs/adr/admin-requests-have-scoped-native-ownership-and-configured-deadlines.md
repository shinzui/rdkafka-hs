---
type: Architecture Decision Record
title: Admin requests have scoped native ownership and configured deadlines
description: Topic administration retains the native client, scopes each owned request and result, validates C representations, and follows the configured native request timeout.
timestamp: 2026-10-24T07:10:27Z
generated:
  by: codex-cli/gpt-6.1-sol
  at: "2026-10-24T07:10:27Z"
docId: ADR-11
status: Accepted
date: 2026-10-24
originatingPlan: docs/plans/8-apply-topic-creation-configuration-safely.md
---

# Admin requests have scoped native ownership and configured deadlines

## Context

The inherited topic constructor applied settings through `undefined`, and failed configuration during acquisition could lose an already allocated request. Native result/queue/options lifetimes depended on garbage collection. A one-second idle poll also discarded the native request deadline, and request-level errors were not decoded. The repair's broker and native ownership regressions expose these paths, plus integer wrapping and NUL name truncation.

## Decision

A topic-admin operation keeps its native client alive through request completion with `withForeignPtr`. Applications continue to serialize explicit client teardown with other operations, as required by [ADR-1](consumer-queue-and-teardown-ownership.md). Garbage-collection retention does not replace that application constraint.

Each application-owned request, options object, queue reference and event has a scoped release boundary. The raw NewTopic request has one manual destroy path inside its bracket; configuration runs in the bracket body. Handles that already own ForeignPtr finalizers use explicit `finalizeForeignPtr`, consuming that existing finalizer. They never gain a second independent deallocator. Native work may retain its own copied request or queue reference after application release.

Native event results and the client's read-only configuration are borrowed views of their owning event/client. Decode result strings and errors while the owner remains alive; do not attach native destroy finalizers to borrowed views. Require the exact single-topic response for the single-topic public request, and preserve both request-level and per-topic errors.

`socket.timeout.ms` supplies the native admin request budget. Idle one-second polls retry within a monotonic deadline, with one additional poll quantum for terminal native error delivery. A timeout leaves the broker outcome unknown. This preserves bounded operations without treating an idle poll as a failed request. No new public timeout/admin API is introduced.

Reject embedded NUL C-string inputs and integer values that cannot be represented by C before native allocation. Other validation follows librdkafka and the broker. Unknown configuration-key acceptance is broker-specific: pass supplied arguments unchanged and test the declared broker policy rather than installing a client whitelist.

## Consequences

[EP-8](../plans/8-apply-topic-creation-configuration-safely.md) provides native call counters, controlled failure/response stages and a retained-request positive control in the integration executable. Counters measure application-owned references and filter internal native-library calls; dedicated leak/invalid-access diagnostics remain required by [ADR-10](release-coverage-requires-behavior-and-ownership-evidence.md). Full initiative and compatibility acceptance remains open under [MasterPlan 2](../masterplans/2-fix-inherited-release-review-defects.md).

Administration works through producer and consumer clients, including after graceful consumer close. Generic `HasKafka` operations retain only the native client pointer. Persistent callback registrations must therefore remain owned through that pointer's native teardown, including when the surrounding client record becomes unreachable. EP-11 owns that implementation and its callback collection evidence.
