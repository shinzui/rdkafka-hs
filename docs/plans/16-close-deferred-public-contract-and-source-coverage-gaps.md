---
id: 16
slug: close-deferred-public-contract-and-source-coverage-gaps
title: "Close deferred public-contract and source-coverage gaps"
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

# Close deferred public-contract and source-coverage gaps

## Purpose / Big Picture

Users can inspect every public API contract and every uncovered handwritten path, see the actual assertions or a source-proven applicability explanation, and reproduce any remaining regression. This finishes exhaustive qualification deferred from the release; it does not add public APIs or invent cases merely to increase counts.

## Progress

- [ ] M1: Reconcile current registered tests and source-bound evidence with every deferred public-contract row without duplicating existing coverage.
- [ ] M2: Close reachable handwritten source/binding gaps with exact regressions and effective semantic/ownership controls; preserve justified exceptions.
- [ ] M3: Investigate the historical seek transition and pass the strict full inventory/coverage gate, with no unresolved owned path.

Not Started. This plan preserves deferred requirements and existing evidence; no implementation result is implied.

## Surprises & Discoveries

The earlier full checkpoint `d466235` had 35 open contract rows and 255 unexplained handwritten misses. Later input, metadata, commit, queue, constructor and partition-list tests already address many rows. The preserved current full run passes 786 registered cases plus four actual nonthreaded cases and leaves 192 unexplained handwritten ticks. The matrix still has 35 open historical rows, many with tests awaiting reconciliation; these are not 35 entirely unimplemented features. See `docs/testing/deferred-validation-snapshot.json`. `coverage-classification-remap.json` preserves 334 classifications with 33 moved coordinates; reuse only identical source lines. `legacy-seek-evidence.json` explains native State's inactive-fetcher predicate but does not identify the historical event that stopped fetching.

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

M1 starts by reading `docs/testing/coverage-matrix.json`, `docs/testing/coverage-evidence.json`, `dist-coverage/evidence.json` and the compiler-derived registered test list. Reconcile exact assertions before adding tests. Give every remaining row an owner and concrete missing behavior. Current checker code restricts owner IDs to the original repair plans; extend validation deliberately if new IDs are recorded, retaining controls that reject missing/unowned/stale evidence. Keep historical ownership and transferred responsibility distinguishable. If logs under ignored build directories are absent, preserve the checked-in manifest and reproduce the case; do not invent results.

M2 examines every unclassified handwritten expression/function/condition in the combined report, including c2hs wrappers and zero-count unlinked modules. Keep generated enum/marshalling boilerplate separate. Add tests for reachable behavior, preserving exact records, offsets, fields, errors and native allocation balance. Likely starting points are unoriginated attributed errors, logical offset sentinels, queue acquisition failures and lazy unit-return expressions; first inspect the current report because later tests may already cover them. Source-prove unused private helpers, native-impossible results and compiler forwarders individually. Never exempt an entire line whose other missing expressions remain reachable. Recheck the four checked-in partial audit manifests and current coordinates; do not broaden them merely to clear a counter. Extend existing integration modules, C fixtures and the Haskell mutation families, using counted native ownership and pending-cancellation barriers for critical branches.

M3 keeps all ten inherited seek scenarios, fresh active-fetcher fixtures, explicit unassignment/State/reassignment checks and repeated replay. The old failure is `dist-coverage/failures/ep8-sync-seek-state.log`, described and hashed in `docs/testing/legacy-seek-evidence.json`. Native 2.15.0 commit `9a94e11452cdeb0a844db44ee5dd01ccbe17d3ab` returns State when a fetcher is inactive; an earlier assignment query is not a later guarantee. Use bounded probes with native topic/fetch/group diagnostics and an operation trace to find the transition. The short max-poll hypothesis already failed to reproduce State; do not present it as the cause. Close this investigation only with a reproducible supported transition plus a justified regression/fixture repair, or concrete evidence that accounts for the saved condition. If the exact historical cause remains unknowable, keep that limitation explicitly open for a user scope decision; passing reruns are insufficient.

Finally collect normal and mapped whole-candidate results, actual nonthreaded runtime coverage and the existing tooling controls. Strict closure is required for this plan's rows; cross-environment evidence is consumed later by EP-20, without silently closing it here.

## Concrete Steps

Run existing baseline commands from the repository root, inside the primary shell or the selected actual environment:

```bash
nix develop
cabal build all -f examples -f integration
cabal test rdkafka-hs-test rdkafka-hs-integration -f integration --test-show-details=direct --test-options='--quickcheck-tests=1000 --quickcheck-replay=20261024'
cabal check
```

Use exact named Tasty selections during implementation, then the required complete candidate checks. A zero-test selection, compilation failure or skipped environment is not passing behavioral evidence. Keep ordinary tests in Cabal/Tasty and extend `scripts/test-mutations.hs` for semantic sensitivity checks; preserve its existing families. Temporary editing/probe scripts may be used freely, but minimize permanent runners.

```bash
nix develop -c python3 scripts/test-coverage-tools.py
nix develop -c python3 scripts/collect-coverage.py
nix develop -c python3 scripts/check-coverage.py --allow-open
```

The temporary `--allow-open` run checks freshness/inventory while other children remain open. At final comprehensive qualification EP-20 runs the same checker without that option. Capture the remaining missing keys from the emitted report; match classifications to exact source spans before accepting them. Run the relevant existing mutation family through `nix develop -c just test-mutations FAMILY`; compiled mutations must fail their named intended assertion.

## Validation and Acceptance

Every public row owned here has an exact positive case and applicable error/boundary/mode/lifetime cases. Every handwritten missing path is either exercised or individually source-proven unreachable/inapplicable. No known correctness defect, unexplained owned miss or failed fixture is relabeled as passing. Normal and mapped runs pass, selected mutations fail their intended assertions, and the seek investigation has its own evidence rather than a retry count. Other required environment rows remain visibly owned by EP-17/18/19 until EP-20 integrates them.

## Idempotence and Recovery

Each run uses a unique prefix and restores only resources it owns. Close/destroy clients before deleting their topics or closing borrowed descriptors. Install cleanup before faulting a broker; use `finally` to restore it even if assertions or cancellation fail. Keep bounded waits and preserve exact errors, seeds, operation traces and logs when a test fails. Retry only after diagnosing the failed precondition or defect; a green rerun does not erase the failure. Never delete another run's topics, reset a shared cluster, alter machine-global 9092, purchase infrastructure, publish, tag or push. Preserve the unrelated `.seihou/manifest.json` edit.

## Interfaces and Dependencies

Whole accepted primary candidate from MasterPlan 2 is the hard prerequisite. Integration dependencies with EP-17, EP-18 and EP-19 are evidence schema and current source fingerprints, not whole-child blocking dependencies. Own `docs/testing/coverage-matrix.json`, source audit manifests and any needed coverage-checker schema changes. EP-20 consumes these outputs and enforces complete closure. Update `docs/testing/coverage.md` and relevant ADRs when a durable applicability or ownership rule is established.

Commits use Conventional Commits with `MasterPlan: docs/masterplans/3-complete-deferred-coverage-and-compatibility-validation.md`, `ExecPlan: docs/plans/16-close-deferred-public-contract-and-source-coverage-gaps.md` and `Intention: intention_01m5p37ej8e3h92gkf2wp9j6qa`. Preserve original provenance and append later author/reviewer entries through the plan skill scripts.
