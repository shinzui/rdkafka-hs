---
type: Architecture Decision Record
title: Release validation has explicit primary and expanded scopes
description: Preserve all implemented repairs and tests while qualifying the immediate release on its demonstrated primary tuple and tracking comprehensive coverage and compatibility separately.
timestamp: 2026-10-24T23:22:39Z
generated:
  by: codex-cli/gpt-6.1-sol
  at: "2026-10-24T23:22:39Z"
docId: ADR-27
status: Accepted
date: 2026-10-24
originatingPlan: docs/masterplans/3-complete-deferred-coverage-and-compatibility-validation.md
---

# Release validation has explicit primary and expanded scopes

## Context

The six-defect repair grew into a full public-API/source-path audit and Linux, newer toolchain, multi-broker Apache, security and native-leak qualification. It found additional defects, whose fixes and regression tests are retained. On 2026-10-24 the user explicitly selected narrowing EP-14 and EP-15 to the demonstrated primary environment, requested a comprehensive follow-up MasterPlan, and required preservation of work already started.

## Decision

[MasterPlan 2](../masterplans/2-fix-inherited-release-review-defects.md) completes the immediate candidate against its demonstrated Apple Silicon Darwin/GHC 9.12.4/librdkafka 2.15.0/private Redpanda 26.2.1 tuple. Retain every implemented fix, registered test, native counter/barrier, semantic mutation, source classification and historical failure manifest. User choices for later subscription properties and manual sync callback polling remain unchanged.

Primary acceptance requires all six repairs and subsequent confirmed defects fixed, complete normal and mapped registered suites, actual nonthreaded checks, existing semantic/tooling controls, focused native diagnostic evidence with effective controls, Cabal/binding/documentation/Nix checks, and clean source-distribution build/test replay. The primary inventory command explicitly uses `scripts/check-coverage.py --allow-open` to validate exports, ownership, evidence hashes and freshness while deferred rows remain visible. Do not change the strict checker to pass an incomplete comprehensive matrix. No known failing regression may be excused as deferred exhaustive coverage.

[MasterPlan 3](../masterplans/3-complete-deferred-coverage-and-compatibility-validation.md) owns exhaustive public-contract/source/binding closure, further historical seek-trigger investigation, Linux and newer released toolchain/native tuples, effective native leak/header lifetime diagnostics, actual Apache leader/outage/group/transaction recovery, real authentication/TLS/ACL/credential reconnect, and the complete fail-closed compatibility/source-package gate. `docs/testing/deferred-validation.md` and its source-bound JSON snapshot retain every transferred obligation and reusable asset. Deferred means unqualified, not tested or inapplicable.

The complete qualification standard in [ADR-10](release-coverage-requires-behavior-and-ownership-evidence.md) applies to MasterPlan 3. Its previous all-environment/every-path requirement no longer blocks the explicitly narrowed primary release. README and evidence must say exactly which tuple and scenarios ran. Broad bounds, plaintext setter checks, simulated error frames, counters and Darwin AddressSanitizer with leak detection disabled must not become broader compatibility, security or leak-freedom claims.

## Consequences

The release is bounded by a concrete tested candidate, while comprehensive validation remains visible, resumable and separately gated. Existing work is reused according to matching source/configuration fingerprints; bookkeeping does not invalidate source identity. Missing required environments still block MasterPlan 3 rather than being waived there. Distribution/version selection/publication remain separately authorized under [ADR-2](release-validation-before-publication.md); this decision authorizes no upload, tag or push.
