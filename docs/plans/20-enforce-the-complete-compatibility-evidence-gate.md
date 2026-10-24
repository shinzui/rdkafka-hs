---
id: 20
slug: enforce-the-complete-compatibility-evidence-gate
title: "Enforce the complete compatibility evidence gate"
kind: exec-plan
created_at: 2026-10-24T23:17:06Z
intention: "intention_01m5p37ej8e3h92gkf2wp9j6qa"
master_plan: "docs/masterplans/3-complete-deferred-coverage-and-compatibility-validation.md"
provenance:
  created_by:
    model: "gpt-6.1-sol"
    harness: "codex-cli"
    at: 2026-10-24T23:17:06Z
---

# Enforce the complete compatibility evidence gate

## Purpose / Big Picture

Users and release maintainers get one fail-closed command that checks the entire declared qualification scope on the exact candidate, including clean source-package execution. The command distinguishes the earlier primary release from comprehensive validation and cannot succeed by silently skipping an environment.

## Progress

- [ ] M1: Implement a complete matrix/inventory gate that rejects missing, unavailable, failed and stale evidence with effective negative controls.
- [ ] M2: Replay required package/tests/examples from a clean source distribution and integrate all accepted child evidence.
- [ ] M3: Pass the comprehensive gate on one frozen candidate and publish accurate tested-environment/applicability documentation.

Not Started. This plan preserves deferred requirements and existing evidence; no implementation result is implied.

## Surprises & Discoveries

The current strict coverage checker already rejects open contract rows and unexplained handwritten misses. The user-authorized primary release validates freshness using its explicit --allow-open mode and a transferred-obligation ledger. Do not weaken the strict command or rewrite deferred requirements as passing. This child finishes the broader gate once its producing children have completed.

## Decision Log

2026-10-24: The user explicitly deferred exhaustive coverage and broader compatibility from EP-14/15, requested a comprehensive follow-up, and required preservation of work already started. Retain existing implementations and evidence; start this child only after its baseline/dependency conditions are met.

## Outcomes & Retrospective

Planning only. Record actual results and remaining limits during implementation. Before completion, distill durable applicability, environment and ownership decisions into the profiled ADR bundle.

## Context and Orientation

This repository is a Haskell wrapper around librdkafka. Public modules live under `src/Kafka`; handwritten native wrappers are in `src/Kafka/Internal/RdKafka.chs`. Unit and broker suites are `rdkafka-hs-test` and `rdkafka-hs-integration`, registered by `test/Main.hs`, `test-integration/Main.hs` and `rdkafka-hs.cabal`. `test-integration/Kafka/TestEnv.hs` provides run-prefixed private topics, strict delivery assertions and cleanup. The demonstrated primary fixture is Apple Silicon Darwin, GHC 9.12.4, librdkafka 2.15.0 and private Redpanda 26.2.1 at 127.0.0.1:39092/admin 39644. Verify actual evidence rather than inferring a loaded library from build flags.

[MasterPlan 3](../masterplans/3-complete-deferred-coverage-and-compatibility-validation.md) transfers deferred requirements from [MasterPlan 2](../masterplans/2-fix-inherited-release-review-defects.md). The accepted primary candidate is a hard prerequisite to implementation, not to drafting this plan. All 786 currently registered cases, C counters/barriers, source classifications and the single Haskell mutation runner remain assets. Do not remove tests or restart completed repairs. `docs/testing/deferred-validation.md` identifies outstanding obligations; current fingerprints decide which evidence can be reused.

[ADR-27](../adr/release-validation-has-explicit-primary-and-expanded-scopes.md) separates primary release acceptance from this expanded work. [ADR-10](../adr/release-coverage-requires-behavior-and-ownership-evidence.md) requires precise behavioral/ownership evidence here. [ADR-5](../adr/integration-tests-run-against-a-project-local-redpanda.md) prohibits shared-broker disruption; [ADR-1](../adr/consumer-queue-and-teardown-ownership.md) requires one native owner and callback-safe teardown. The current toolchain floor remains in [ADR-4](../adr/toolchain-floor-is-ghc-9-12-and-librdkafka-2-15.md).

Use Mori before dependency APIs: `mori registry search librdkafka`, `mori registry show confluentinc/librdkafka --full`, and `mori registry docs confluentinc/librdkafka` locate `mori://confluentinc/librdkafka`. Its C source/header files are project-relative `librdkafka/src/rdkafka.h` and `librdkafka/src/`; source artifact URIs are pending. Discover other packages through Mori and verify authoritative registry/releases/tags before choosing any new bound, pin or workaround. Never traverse `/nix/store` or filesystem root.

## Plan of Work

M1 defines `just test-compatibility` as the single full qualification entry point using the existing toolchain. It preflights or verifies every declared required tuple and scenario, compares exact source/config fingerprints and artifact hashes, checks registered nonzero tests and actual loaded versions, then invokes strict contract/source coverage validation. A previously passing row from changed source/config is stale until revalidated; genuinely unaffected evidence may be reused with an explicit source-scope proof. Environment unavailable, skipped scenario, missing artifact and wrong loaded native library are failures. A primary-only report must be labeled partial and cannot satisfy this command. Avoid a script per tuple: prefer one Haskell coordinator or a small existing-tool composition, with ordinary behavior tests remaining Cabal suites.

Prove the gate with independent negative controls: remove a required tuple, set a row unavailable, alter a source fingerprint, corrupt an artifact hash, provide zero tests, substitute the wrong loaded version, reopen a handwritten miss and remove an ownership diagnostic control. Each must fail for the intended reason; the complete unchanged manifest passes. This verifies evidence mechanics, not the library behavior, so keep the two evidence categories separate.

M2 creates `cabal sdist`, checks its exact contents, extracts to a fresh ignored work directory and builds library/examples/unit/integration tests without source paths back to the checkout. Run the complete required primary suite and representative tuple-specific packaged scenarios, plus bounded examples and deprecated wrappers using isolated broker/topic/group arguments. Verify package inclusion of every Haskell and C test source, the mutation runner and coverage tools. Retain existing nonthreaded runtime proof and repeat affected controls when changes invalidate it. EP-16 supplies strict source/path closure; EP-17 supplies Linux/newer/native-memory proof; EP-18 and EP-19 supply actual broker/security behavior. No child is silently partially accepted by the integration step.

M3 freezes the candidate, runs normal optimized and mapped builds, all required compatibility scenarios or validated matching source-bound evidence, strict inventory/miss closure, effective diagnostic and semantic controls, Cabal/binding/documentation/Nix checks, and the clean package replay. Record a concise `docs/testing/comprehensive-validation-evidence.json` linking commands, tuple data, test identities, fingerprints and artifact hashes. Update `docs/testing/coverage.md`, README and relevant ADRs so tested claims exactly match accepted executions and applicability limits. Mark the follow-up MasterPlan complete only when the command passes with all required rows present; publication remains a separately authorized task.

## Concrete Steps

Run existing baseline commands from the repository root, inside the primary shell or the selected actual environment:

```bash
nix develop
cabal build all -f examples -f integration
cabal test rdkafka-hs-test rdkafka-hs-integration -f integration --test-show-details=direct --test-options='--quickcheck-tests=1000 --quickcheck-replay=20261024'
cabal check
```

Use exact named Tasty selections during implementation, then the required complete candidate checks. A zero-test selection, compilation failure or skipped environment is not passing behavioral evidence. Keep ordinary tests in Cabal/Tasty and extend `scripts/test-mutations.hs` for semantic sensitivity checks; preserve its existing families. Temporary editing/probe scripts may be used freely, but minimize permanent runners.

After all four hard dependencies are Complete, implement and run:

```bash
nix develop -c python3 scripts/check-coverage.py
nix develop -c just test-compatibility
nix develop -c cabal check
nix develop -c cabal sdist
nix develop -c just check
```

`just test-compatibility` is a planned output, not an existing passing command. Document the clean extraction directory and exact builds/tests in the accepted manifest. Use test tooling controls to prove rejection paths; a printed table with unchecked claims is not a gate.

## Validation and Acceptance

All EP-16/17/18/19 deliverables are accepted on compatible exact candidate evidence, strict public-contract/handwritten coverage closure passes, every declared required tuple/scenario is validated, gate negative controls work, and clean source-distribution execution succeeds. No source changes occur during collection. No unsupported blanket native-leak claim or untested environment claim appears in documentation. Package publication/tagging/push is not part of completion.

## Idempotence and Recovery

Each run uses a unique prefix and restores only resources it owns. Close/destroy clients before deleting their topics or closing borrowed descriptors. Install cleanup before faulting a broker; use `finally` to restore it even if assertions or cancellation fail. Keep bounded waits and preserve exact errors, seeds, operation traces and logs when a test fails. Retry only after diagnosing the failed precondition or defect; a green rerun does not erase the failure. Never delete another run's topics, reset a shared cluster, alter machine-global 9092, purchase infrastructure, publish, tag or push. Preserve the unrelated `.seihou/manifest.json` edit.

## Interfaces and Dependencies

Whole-child hard dependencies: `docs/plans/16-close-deferred-public-contract-and-source-coverage-gaps.md`, `docs/plans/17-validate-linux-and-released-toolchain-native-lifetimes.md`, `docs/plans/18-verify-distributed-broker-failure-and-group-recovery.md` and `docs/plans/19-verify-authenticated-kafka-and-credential-recovery.md`. Own the full gate, combined manifest, clean package replay and final documentation reconciliation. Consume their schema/fixtures without taking over source repair ownership. Preserve the separate primary release record and its truthful limits.

Commits use Conventional Commits with `MasterPlan: docs/masterplans/3-complete-deferred-coverage-and-compatibility-validation.md`, `ExecPlan: docs/plans/20-enforce-the-complete-compatibility-evidence-gate.md` and `Intention: intention_01m5p37ej8e3h92gkf2wp9j6qa`. Preserve original provenance and append later author/reviewer entries through the plan skill scripts.
