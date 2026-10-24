---
type: Architecture Decision Record
title: Manual sync callback polling preserves records
description: Preserve manual synchronous callback polling by copying returned messages into a partition-aware FIFO with scoped native ownership.
timestamp: 2026-10-24T20:16:21Z
generated:
  by: codex-cli/gpt-6.1-sol
  at: "2026-10-24T20:16:21Z"
docId: ADR-25
status: Accepted
date: 2026-10-24
originatingPlan: docs/plans/14-cover-the-public-api-and-pure-data-laws.md
---

# Manual sync callback polling preserves records

## Context

Native consumer polling serves callbacks and can also return application records. The explicit sync-mode callback poll destroyed those records: the next application poll received offset one after offset zero had disappeared. The user chose to preserve manual sync callback polling. A simple FIFO also required corrections for seek, assignment and pause operations.

## Decision

Copy returned sync records and attributed errors inside the native-message bracket, then destroy each native message before returning. Keep only Haskell-owned values in a private FIFO associated with the consumer configuration. The FIFO contains no native messages, clients or callbacks, so it does not introduce a client/callback ownership cycle.

Application polls serve ready callbacks while copied results exist, then take the first eligible result. Empty native polls add nothing. Fatal state remains observable through the existing fatal-state check; repeated fatal messages are not accumulated. Async mode retains its separate partition queues.

Publish successful native state changes together with corresponding buffer updates while masked. Assignment replacement clears the FIFO and remembered pause state. Successful or initiated seek members discard copies only for their partitions; failed members retain their previous state. Legacy seeks invalidate each successful member before proceeding, preserving partial success. Pause/resume remember only successful members and defer paused copies while allowing other partitions to progress. Checked eager/cooperative rebalance transitions invalidate their corresponding assignment scopes in both callback modes. Close and destroy clear retained copies.

An explicit callback poll remains a native poll: native position and enabled automatic offset storage advance when native code returns messages. The FIFO preserves those received results for subsequent application polls; it does not change native offset-storage policy.

## Consequences

No public signature or callback-mode precondition changes. Both modes install the checked rebalance bridge; a later user callback replaces its no-op handler while retaining the same state bookkeeping. Actual threaded and nonthreaded runtime checks serve real sync rebalance callbacks and preserve the first record.

[Callback-buffer evidence](../testing/callback-buffer-evidence.json) records the failing record-loss and state-transition probes, complete normal/mapped runs, instrumented native checks and compiled mutations. [EP-14](../plans/14-cover-the-public-api-and-pure-data-laws.md) remains incomplete until its other contracts and coverage obligations are closed. Real multi-consumer handoff/outage scenarios remain owned by EP-15.
