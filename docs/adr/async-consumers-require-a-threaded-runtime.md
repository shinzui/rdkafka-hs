---
type: Architecture Decision Record
title: Async consumers require a threaded runtime
description: Reject async consumer construction before allocation when the executable lacks the threaded runtime, while retaining synchronous consumers and runners.
timestamp: 2026-10-24T16:32:48Z
generated:
  by: codex-cli/gpt-6.1-sol
  at: "2026-10-24T16:32:48Z"
docId: ADR-22
status: Accepted
date: 2026-10-24
originatingPlan: docs/plans/14-cover-the-public-api-and-pure-data-laws.md
---

# Async consumers require a threaded runtime

## Context

The background callback loop starts only when the runtime supports bound threads. A nonthreaded executable nevertheless accepted async construction, including its default mode, leaving subscription callbacks unserved and record consumption timing out. A real nonthreaded executable reproduces the failure, while its synchronous consumer reads the same fixture successfully.

## Decision

Require a threaded runtime for async consumers. Return `KafkaBadSpecification` before allocating a client when async mode is requested without it. The error explains both remedies: link with `-threaded` or select `CallbackPollModeSync`. Apply the same rule to the default mode and explicit async mode. Preserve synchronous construction, record delivery and deprecated runner cleanup in both runtimes.

Do not silently change the selected poll mode. In particular, synchronous batch polling retains its documented unsupported result. `effectiveCallbackPollMode` continues to describe the supplied properties.

## Consequences

Applications using the default must link with `-threaded`. Nonthreaded applications can explicitly choose synchronous polling. Validation builds and runs both actual runtime variants; a compiled mutation removing the guard must fail in the nonthreaded variant. Source-level inspection or a threaded run alone cannot establish the rejected branch.
