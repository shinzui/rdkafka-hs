---
type: Architecture Decision Record
title: Diagnostics borrow handles and scope native dumps
description: Preserve caller handle ownership while flushing and closing an owned native stream, and bracket native configuration dump arrays through conversion.
timestamp: 2026-10-24T16:26:13Z
generated:
  by: codex-cli/gpt-6.1-sol
  at: "2026-10-24T16:26:13Z"
docId: ADR-21
status: Accepted
date: 2026-10-24
originatingPlan: docs/plans/14-cover-the-public-api-and-pure-data-laws.md
---

# Diagnostics borrow handles and scope native dumps

## Context

The print functions converted the caller's handle directly to a file descriptor, consuming the handle. Their native streams and allocated mode strings were never freed. Configuration dumps were freed only after normal conversion, so cancellation immediately after acquisition lost the array.

## Decision

Duplicate the supplied handle and transfer only its duplicate descriptor to `fdopen`. Close that descriptor if stream creation fails. Bracket a successful native stream with checked `fclose`, which flushes its output and closes the duplicate. Keep the caller's handle open, report native stream failures and preserve output ordering through the flushed shared file position. Callers must serialize other access while printing. Retain the native client through diagnostics and stream cleanup.

Bracket dump acquisition, copying and native array destruction together. Force the resulting strict map within that scope. Cancellation and conversion errors must release the entire native allocation. Keep native dump contents and public signatures unchanged.

## Consequences

[EP-14](../plans/14-cover-the-public-api-and-pure-data-laws.md) verifies caller-handle usability, output ordering and flushing, genuine native stream/array counters, deliberate retained-array detection, and observed cancellation at global/topic dump acquisition. Failure probes cover both stream creation and final flush without consuming the caller's handle. A configuration dump remains an inspection of its retained configuration, not proof that a live client uses it; effective producer behavior is covered separately by [ADR-12](producer-settings-merge-explicit-topic-keys-after-global-keys.md).
