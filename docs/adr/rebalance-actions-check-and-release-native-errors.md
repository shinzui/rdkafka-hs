---
type: Architecture Decision Record
title: Rebalance actions check and release native errors
description: Report failed native assignment actions through callback exception containment and release cooperative error objects before returning from the callback.
timestamp: 2026-10-24T21:45:25Z
generated:
  by: codex-cli/gpt-6.1-sol
  at: "2026-10-24T16:09:39Z"
docId: ADR-19
status: Accepted
date: 2026-10-24
originatingPlan: docs/plans/14-cover-the-public-api-and-pure-data-laws.md
---

# Rebalance actions check and release native errors

## Context

Both rebalance protocols discarded assignment errors. Cooperative incremental assignment and revocation additionally return owned error objects, which leaked. The callback then published an after-event even when the native action failed.

## Decision

Check eager assignment return codes and bracket cooperative error objects around decoding. Native NULL means success and is safe to destroy. Release an owned error even when its code cannot be decoded.

Raise a failed action inside the existing contained native callback boundary. It reports the exception on a separate Haskell thread through the runtime's uncaught-exception handler, as specified in [ADR-15](native-clients-own-persistent-haskell-callback-bridges.md). Preserve before-events, and emit after-assignment or after-revocation events only after native success. Public callback signatures remain unchanged.

## Consequences

Observe assignmentLost during RebalanceBeforeRevoke, before the checked native unassignment. Serving revocation can clear the native loss state, so a later snapshot is not a reliable oracle for the earlier callback phase. Static-member fatal fencing need not deliver a revocation event. Actual True loss-phase evidence remains an explicit EP-15 eviction obligation, separate from fatal-error observations and ordinary group handoffs.

Real cooperative consumers verify successful assignment, record delivery and revocation in both callback modes. Isolated probes invoke the installed callback bridge with native partition lists and controlled native failures; counters verify error destruction, including decoder exceptions. Those probes establish callback failure semantics and ownership. Multi-consumer handoffs and broker failures remain separate EP-15 obligations.
