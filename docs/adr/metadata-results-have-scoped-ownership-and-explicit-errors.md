---
type: Architecture Decision Record
title: Metadata results have scoped ownership and explicit errors
description: Copy and release native metadata and group lists within one scope, preserve nested query errors, and return unsupported group states as errors.
timestamp: 2026-10-24T16:44:52Z
generated:
  by: codex-cli/gpt-6.1-sol
  at: "2026-10-24T16:44:52Z"
docId: ADR-23
status: Accepted
date: 2026-10-24
originatingPlan: docs/plans/14-cover-the-public-api-and-pure-data-laws.md
---

# Metadata results have scoped ownership and explicit errors

## Context

Native metadata and group-list calls return owned results. Wrapping them with finalizers only after an unmasked native call loses the result if cancellation arrives at native return. Relying on garbage collection also delays release after copying. Topic convenience queries lost missing-topic errors, timestamp lookups ignored partition errors, and unfamiliar group-state strings raised a partial-decoder exception.

## Decision

Retain the client and bracket native acquisition, complete copying and finalization together. Mask acquisition through ForeignPtr registration. Keep finalizers as a fallback and explicitly finalize each result after conversion. Temporary topic and partition-list owners remain nested inside the retained client scope.

Preserve native overall errors, topic errors used by convenience queries, and timestamp-query member errors. Return a descriptive `KafkaError` for unsupported native group-state strings. Recognize both legacy `AwaitingSync` and native `CompletingRebalance` as `GroupAwaitingSync`. Preserve the public types and result shapes.

## Consequences

[EP-14](../plans/14-cover-the-public-api-and-pure-data-laws.md) verifies topic/partition identity, replica and leader consistency, exact watermarks, timestamp boundary results, missing-topic and mixed-request errors, and real group identity/lifecycle. Controlled group-list states use genuinely allocated native results. Isolated cancellation children count native acquisition/destruction and terminate on a failed ownership assertion; they never compete with a ForeignPtr finalizer to free a managed result. A separate retained-result control is owned and cleaned up entirely by the native fixture.

An unreachable bootstrap endpoint returns native `Transport` before a broker can accept a metadata request. That differs from timing out after request submission; tests must preserve the actual native outcome instead of labeling every elapsed deadline `TimedOut`.
