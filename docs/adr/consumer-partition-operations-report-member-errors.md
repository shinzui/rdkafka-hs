---
type: Architecture Decision Record
title: Consumer partition operations report member errors
description: Preserve overall native errors before the first failing partition and retain successful effects of mixed consumer requests.
timestamp: 2026-10-24T21:45:25Z
generated:
  by: codex-cli/gpt-6.1-sol
  at: "2026-10-24T16:18:45Z"
docId: ADR-20
status: Accepted
date: 2026-10-24
originatingPlan: docs/plans/14-cover-the-public-api-and-pure-data-laws.md
---

# Consumer partition operations report member errors

## Context

Native pause, resume, offset storage and position calls can succeed overall while setting individual partition errors. Public wrappers discarded those fields. A mixed offset-store request can store a valid member while failing another.

## Decision

Keep the public result types. Check an overall error first, then return the first failing member in native input order. Apply this rule to pause, resume, store, position and committed-offset results, consistent with the seek policy in [ADR-14](seek-reports-ordered-member-errors-and-preserves-initiation-semantics.md). Do not roll back successful native members of a mixed request.

An unknown position is not intrinsically an error: librdkafka can create local partition state and return the invalid-offset sentinel without setting an error. Preserve that result. Tests must distinguish this valid native behavior from an actual native member failure.

Deprecated seek has an active-fetcher precondition. A successful assignment
query is a snapshot and does not establish that precondition for a later seek.
Native `State` is a supported result and must propagate; a stopped fetcher is
not made active by querying its assignment. Positive fixtures therefore use a
fresh grouped consumer and verify a record immediately before seeking. Keep
separate unassignment/State/reassignment tests and exact replay checks in both
callback modes. The historical saved log does not identify which event stopped
its fetcher; do not infer an external cause from later green runs.

## Consequences

A representable but unavailable assignment can be accepted before metadata resolves; the later availability error arrives through the native error callback. Test the reported code and recovery rather than demanding immediate rejection. Commit callbacks can retain Invalid offset entries for assigned partitions that were not submitted as valid broker offsets. Preserve those callback entries and independently verify each broker-committed offset. Commit-all only submits stored positions ahead of the committed position; a backward store can succeed without causing a later commit-all to rewind. Explicit partition commits have separate semantics.

[EP-14](../plans/14-cover-the-public-api-and-pure-data-laws.md) checks exact record delivery after pause/resume, effects of mixed requests, stored versus broker-committed next offsets, assignment replacement/unassignment and native error propagation in both callback modes. Empty operations retain native semantics. A green repeated deprecated-seek check does not explain or close the separately saved intermittent state failure.
