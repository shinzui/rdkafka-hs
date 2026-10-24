---
type: Architecture Decision Record
title: Polled messages have one scoped owner
description: Mask native message acquisition, copy within its ownership scope and release every returned message before the client can be torn down.
timestamp: 2026-10-24T15:36:38Z
generated:
  by: codex-cli/gpt-6.1-sol
  at: "2026-10-24T15:36:38Z"
docId: ADR-18
status: Accepted
date: 2026-10-24
originatingPlan: docs/plans/14-cover-the-public-api-and-pure-data-laws.md
---

# Polled messages have one scoped owner

## Context

The native poll bindings return ForeignPtrs without message finalizers. Conversion previously destroyed its current message, but cancellation between acquisition and conversion lost it. Batch conversion also lost every unprocessed member after interruption or a decoder exception. Header-read failures were silently replaced with empty headers.

## Decision

Scope single-message acquisition, copying and destruction together, with acquisition masked. Callback-event polling uses the same ownership helper while retaining its discard behavior. Keep the client alive through message cleanup.

A batch owns its complete returned list. Its converters borrow members; the batch releases every member even if conversion never reaches it. Avoid individual message finalizers that would compete with this group owner. Native batch counts retain their signed return type, normal empty polls return an empty list, and acquisition failures remain errors. Reject negative sizes and pointer-array multiplication overflow before entering native code.

Native header absence remains empty headers. Actual header-read failures become poll errors; attributed polling retains the native topic, partition and offset. Copy all successful record contents before native destruction, consistent with [ADR-6](consumed-headers-preserve-wire-order.md).

## Consequences

[EP-14](../plans/14-cover-the-public-api-and-pure-data-laws.md) uses actual native message counters, a retained-object control, observed cancellation barriers and decoder failures. The inherited code fails ten targeted checks; repaired polling passes those cases and its size/timeout/error boundaries. Native instrumentation and semantic mutations complement Haskell coverage. Caller serialization with client teardown remains the precondition established by [ADR-1](consumer-queue-and-teardown-ownership.md).
