---
type: Architecture Decision Record
title: Delivery callbacks transfer once and keep exceptions in Haskell
description: Mask the enqueue ownership decision, release rejected callback roots immediately, and contain delivery callback exceptions within Haskell threads while preserving per-message delivery.
timestamp: 2026-10-24T10:30:16Z
generated:
  by: codex-cli/gpt-6.1-sol
  at: "2026-10-24T08:17:05Z"
docId: ADR-13
status: Accepted
date: 2026-10-24
originatingPlan: docs/plans/9-honor-producer-settings-and-release-rejected-callbacks.md
---

# Delivery callbacks transfer once and keep exceptions in Haskell

## Context

An immediate enqueue rejection produces no delivery report. The inherited producer nevertheless left its per-message `StablePtr` allocated. Its global delivery callback also ran before the per-message root's release scope: a user exception could skip the message callback and unwind through a native callback frame. Another polling thread can serve a successful enqueue's report before the enqueue caller returns.

## Decision

Mask asynchronous exceptions from per-message callback allocation through the native enqueue ownership decision. Immediate rejection leaves the root owned by the caller, which frees it exactly once. The native API transfers ownership only when its returned error pointer is NULL. A non-null error object means rejection: free the root before decoding its error code and bracket destruction of that object. A conversion exception therefore cannot retain the rejected root. A contradictory non-null success-code object returns `KafkaInvalidReturnValue`. Preserve native error-object codes, including `UNKNOWN`; these are already translated error codes and must not be replaced by system `errno`. The caller must never free an accepted root, including when cancellation is pending or another poller has already delivered the report.

Delivery handling brackets the root before decoding the report or invoking user callbacks. It resolves the Haskell closure, invokes the global callback on the polling thread, and forks the per-message callback before releasing the native root. The forked Haskell thread then retains the closure for its own lifetime. It runs unmasked so ordinary Haskell cancellation and exception behavior remain available.

A global callback exception does not prevent per-message scheduling. Exceptions arising in the native delivery callback are contained and reported by throwing on a separate unmasked Haskell thread, using the runtime's uncaught-exception policy. They never unwind through a native callback frame. Per-message callback exceptions use the ordinary policy of their own Haskell thread. Throwing from a global callback cannot be used to abort the surrounding native poll or flush operation; this includes asynchronous exceptions delivered while that callback runs.

Per-message `StablePtr` roots and persistent callback `FunPtr` registrations are separate resources. This decision does not claim persistent registration teardown is fixed; EP-11 owns it under [ADR-1](consumer-queue-and-teardown-ownership.md).

## Consequences

[EP-9](../plans/9-honor-producer-settings-and-release-rejected-callbacks.md) checks weak-reference retention and collection, repeated real queue-full rejections, cancellation observed at native barriers before and after acceptance, delivery before the enqueue caller returns, and capture retention while a forked callback runs. Throwing callbacks run in isolated probes, whose coverage is retained and merged with the parent suite. Native mock protocol requests establish actual in-flight state for controlled purge cases; real-broker round trips remain separate evidence.

The global callback remains synchronous and should remain short. The per-message callback remains forked. Signatures and producer close/flush behavior are unchanged. Native leak and invalid-access diagnostics and the support matrix remain required by [ADR-10](release-coverage-requires-behavior-and-ownership-evidence.md).
