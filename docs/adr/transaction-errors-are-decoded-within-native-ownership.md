---
type: Architecture Decision Record
title: Transaction errors are decoded within native ownership
description: Scope native transaction error acquisition, complete decoding and destruction together while retaining public results and recovery flags.
timestamp: 2026-10-24T14:12:24Z
generated:
  by: codex-cli/gpt-6.1-sol
  at: "2026-10-24T14:12:24Z"
docId: ADR-16
status: Accepted
date: 2026-10-24
originatingPlan: docs/plans/12-release-transaction-error-objects.md
---

# Transaction errors are decoded within native ownership

## Context

Transaction operations return caller-owned native error pointers. Reading their code does not destroy them. The inherited initialization, begin and abort wrappers lost these pointers; commit and send-offset decoding released them only after normal conversion. Cancellation and decoding failures need the same ownership boundary as ordinary returns.

## Decision

Each public operation brackets native acquisition, complete decoding and destruction. NULL remains success. Copy the error code and all fatal, retriable and abort-required flags before release, forcing the strict transaction error fields within that scope. Preserve the existing public return types and native classification; callers still choose retry or abort policy.

Sending source offsets separately scopes its temporary partition list and native consumer-group metadata. Commit the source record's next offset, so successful commit exposes output and advances the source together; abort exposes neither.

Use native allocation/destruction counters on returned transaction objects, a deliberate decode-without-destroy control, actual decoder exceptions and observed cancellation barriers. Native error fixtures use the library's own allocation and flag setters rather than copying opaque structure layouts. Real broker fencing, rejected deliveries, commit/abort visibility and source offsets complement these fixtures.

## Consequences

[EP-12](../plans/12-release-transaction-error-objects.md) owns implementation and candidate acceptance. These scopes repair ownership without introducing a generic resource framework or changing lifecycle/retry APIs. Private fixture symbols are diagnostic dependencies, separate from the package's public native API requirements. Native instrumentation and supported environment validation remain explicit evidence obligations; a passing result or stable process memory alone does not prove destruction.
