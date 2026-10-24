---
type: Architecture Decision Record
title: Consumer partition operations report member errors
description: Preserve overall native errors before the first failing partition and retain successful effects of mixed consumer requests.
timestamp: 2026-10-24T16:18:45Z
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

## Consequences

[EP-14](../plans/14-cover-the-public-api-and-pure-data-laws.md) checks exact record delivery after pause/resume, effects of mixed requests, stored versus broker-committed next offsets, assignment replacement/unassignment and native error propagation in both callback modes. Empty operations retain native semantics. A green repeated deprecated-seek check does not explain or close the separately saved intermittent state failure.
