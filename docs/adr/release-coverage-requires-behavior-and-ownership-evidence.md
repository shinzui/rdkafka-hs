---
type: Architecture Decision Record
title: Release coverage requires behavior and ownership evidence
description: Release acceptance requires an explicit tested public-contract and native-ownership inventory, trustworthy fixtures, and source-bound compatibility evidence rather than only passing counts or aggregate coverage.
timestamp: 2026-10-25T17:33:07Z
generated:
  by: codex-cli/gpt-6.1-sol
  at: "2026-10-25T00:02:36Z"
docId: ADR-10
status: Accepted
date: 2026-10-24
originatingPlan: docs/masterplans/2-fix-inherited-release-review-defects.md
---

# Release coverage requires behavior and ownership evidence

## Context

The pre-release review passed 14 unit and 116 integration tests while reproducing five inherited defects and identifying a sixth native error-object leak by source inspection. Existing fixtures ignore enqueue errors or discard consumer errors, and several assertions check only success or collection size. Callback ownership, transaction failures, configuration application, and compatibility have gaps that a passing count conceals. The user requested that the repair plans close coverage gaps across the library.

## Decision

Scope amendment, 2026-10-24: the comprehensive standard below now belongs to
[MasterPlan 3](../masterplans/3-complete-deferred-coverage-and-compatibility-validation.md).
The user explicitly narrowed the immediate release to the demonstrated primary
environment while preserving every implemented repair and test.
[ADR-27](release-validation-has-explicit-primary-and-expanded-scopes.md) defines
that release gate and the deferred-obligation ledger. Open comprehensive rows
remain visible and cannot be relabeled as passing.

A release candidate must have an inventory mapping every supported public behavior and critical native allocation, ownership transfer, error conversion, and release path to precise test assertions and current-source evidence. Include applicable success, failure, boundary, mode, and lifetime cases. A function being called is not proof that its result or cleanup is correct. Re-exports, diagnostics, deprecated entry points, and pure-data laws remain visible in the inventory.

Native ownership observers must identify the current live allocation when an allocator reuses an address. A released address alone is not proof of a double release: later data or error frames can occupy it. Select the live generation, restore its own saved fields, and retire dead address matches when observing uncounted error frames. Preserve a double-release control only when no new allocation has been observed at that address.

Fixtures must preserve unexpected errors and assert exact records, offsets, counts, and results. Meaningful concurrency and asynchronous-exception windows require observed barriers or state, not merely thread launch followed by cancellation. Generated property tests provide replay seeds and minimized failures; native-resource checks include leak/invalid-access evidence with positive controls proving instrumentation is effective. Deliberate semantic mutations must fail the focused behavioral tests, not just compilation.

Haskell Program Coverage reports expression, alternative, and condition coverage for the hand-written Haskell code. Keep generated binding boilerplate separate and classify uncovered hand-written paths. These metrics do not prove native memory safety. Run normal optimized builds as well as instrumented builds. There is no test-count cap or aggregate percentage that excuses a missing supported behavior.

Every handwritten miss classification carries the exact source path and whole-file SHA-256. Validation rejects a changed or missing source binding even during `--allow-open` development checks; a matching coordinate and a newly collected aggregate fingerprint cannot renew an old applicability proof. Transfer unfinished obligations to their follow-up child while preserving `previous_owner`, and derive fallback miss ownership from the current ledger rather than a hard-coded historical owner.

The final gate rejects open, unowned, stale, skipped-required, or unexplained coverage rows. A not-applicable classification requires a concrete documented precondition or source-proven unreachable path. Tooling or environment unavailability is a blocker, not non-applicability. Newly discovered violations of existing supported contracts require regression tests and repair; expected failures cannot make the release gate green.

Record a representative tested-environment matrix covering the current primary Darwin/Redpanda tuple, Linux build and representative broker/native-diagnostic execution, Apache Kafka behavior including leader failover, and a newer compatible released compiler/library tuple when available. Select actual released versions during implementation, verify registry/tag evidence, and record the loaded native-library version. Broad dependency bounds do not establish that every allowed combination is tested. Do not silently narrow supported behavior to avoid a failing matrix case.

All broker disruptions and security tests use run-owned disposable fixtures. The protections in [ADR-5](integration-tests-run-against-a-project-local-redpanda.md) remain: never mutate the machine-global broker. Extending test execution to additional supplied isolated environments does not authorize a general-purpose provisioning system or infrastructure purchases. The implementation must amend environment-specific guidance when those additional fixtures exist. Earlier release distribution and platform history in [ADR-7](first-release-scope-and-distribution.md) remains historical fact.

The complete gate validates required tuple/scenario identities, actual versions,
configuration and artifact hashes, strict source closure and effective diagnostic
controls. Package execution must use an extraction-local Cabal project: all four
component source/build paths and the archived/extracted runtime and tooling bytes
are checked. Merely running Cabal from a subdirectory does not isolate it from
ancestor project discovery. Unchanged behavior may be reused after tooling-only
changes only through exact archive equality of every runtime, build, test and
example source; the current package must include current tooling.

Inspect all retained native allocation stacks and leak kinds. A conservative RTS
root can make a deliberately retained native header reachable and leave the
native tool exit zero. Detect that control through retained header allocation
stacks, with a stock cohort containing none, and preserve the raw exit value.
Neither zero exit nor Darwin ASan with leak detection disabled proves leak freedom.

## Consequences

[MasterPlan 2](../masterplans/2-fix-inherited-release-review-defects.md) adds fixture/inventory, public-contract/property, and failure/compatibility work streams around its five repair plans. The fixtures and coverage commands are prerequisites for the repair evidence; the final candidate requires all children and matrix rows to pass. [ADR-1](consumer-queue-and-teardown-ownership.md) continues to govern client teardown, and [ADR-2](release-validation-before-publication.md) continues to require validation before publication.

[EP-13](../plans/13-make-coverage-evidence-and-test-fixtures-reliable.md) implements the fixture, compiler-export inventory and source-bound coverage infrastructure. Hand-written c2hs wrappers require coverage-only source mapping because GHC ignores redirected source spans; unlinked library modules require matching-hash zero counters so their functions remain visible. Reports retain generated binding boilerplate separately and reject absent hand-written instrumentation. The defect repairs, expanded public-contract coverage and environment matrix remain open in their owning children. Evidence must identify the source/configuration tested; documentation bookkeeping alone must not invalidate source fingerprints. Completion establishes a checked support contract, not proof over all possible inputs, schedules, and future dependency versions.
