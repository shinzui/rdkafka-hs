---
id: 18
slug: verify-distributed-broker-failure-and-group-recovery
title: "Verify distributed broker failure and group recovery"
kind: exec-plan
created_at: 2026-10-24T23:17:06Z
intention: "intention_01m5p37ej8e3h92gkf2wp9j6qa"
master_plan: "docs/masterplans/3-complete-deferred-coverage-and-compatibility-validation.md"
provenance:
  created_by:
    model: "gpt-6.1-sol"
    harness: "codex-cli"
    at: 2026-10-24T23:17:06Z
  revisions:
    - model: "gpt-6.1-sol"
      harness: "codex-cli"
      at: 2026-10-25T00:21:24Z
      mode: "implement"
      note: "Use run-owned Apache topology for first observed leader-change integration probe"
---

# Verify distributed broker failure and group recovery

## Purpose / Big Picture

Users can see exactly how producer delivery, consumption, group membership and transactions behave when a real broker or leader disappears and recovers. This supplies the real distributed-failure proof deferred from the primary single-broker release.

## Progress

- [ ] M1: Create a run-owned Apache Kafka topology and prove a representative leader-change roundtrip with exact record accounting.
- [ ] M2: Verify outage/reconnect, group assignment/eviction/pause/seek and transaction recovery in applicable callback modes.
- [ ] M3: Accept source-bound failure evidence, restored fixtures and broker-policy results for EP-20.

In Progress (2026-10-24). A run-owned three-node Apache Kafka 4.4.0 topology is serving the initial Linux tuple. The initial probe observes leader 2 → 3, ISR [3,1], exact records at offsets 0/1 and committed next offset 2 in both callback modes, then restores the stopped broker and three-member ISR. `../testing/broker-failure-evidence.json` binds the probe and outputs. Permanent Cabal scenarios and the remaining distributed recovery acceptance remain open.

## Surprises & Discoveries

Current tests contain real local consumer fencing, cooperative assignment/revocation, transaction fencing, timeout endpoints and controlled native errors. They do not prove a multi-broker leader election or actual assignmentLost True during group eviction. `API-Kafka-Consumer-assignmentLost` was explicitly transferred from EP-14 to the former EP-15's real eviction requirement; it remains required here. Existing successful/error frames remain reusable regression coverage, not substitutes for the real transitions.

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

M1 discovers and verifies a released Apache Kafka version and its actual deployment API. Prefer the smallest disposable topology that can elect another leader while preserving intended replication guarantees: three brokers, replicated test topics and declared minimum in-sync replicas. Provision only isolated ports/data owned by this run, with no machine-global 9092 changes. Use exact topic/partition/leader/replica observations. Produce identified records, consume exact identities/offsets, stop the current leader, observe actual election and prove the next identified record survives and is consumed according to the documented delivery/transaction guarantee. Exercise this one path before expanding permutations. EP-17 supplies the shared tuple evidence shape; this integration does not require its entire execution matrix to finish first.

M2 checks a bounded complete outage followed by restoration: delivery timeouts/retries, queue limits, callback results and observed reconnection; exact post-recovery records and commits; no silent loss or claimed delivery without a report. Keep duplicate expectations aligned with the chosen idempotence/transaction settings. Verify commit-all and polling errors/recovery rather than only a raw socket timeout. Explicitly cover manual sync callback polling under the same recovery transitions so its copied buffer respects current assignment, successful seeks and paused partitions.

Use multiple actual consumers to test eager and cooperative group handoff, partition addition/revocation, slow/missing polling, eviction and recovery. Observe callback phases and prove assignmentLost True in the native-supported revocation/loss window, with healthy False controls. Do not assume static fencing always emits a revoke callback or immediately changes assignmentLost; prior fixtures disproved that assumption. Check exact partition ownership and offsets, no accidental duplicate handoff and pause/resume or seek effects across assignment changes. Both callback modes apply where the public API supports them.

Exercise transactions across broker/coordinator interruption and producer fencing: initialization retries, begin/produce/commit or abort decisions, native error flags, recovery and read-committed visibility. Distinguish a retriable result from abort-required and fatal outcomes, and prove all returned native errors are destroyed. Existing EP-12 error frames test allocation ownership; retain them while adding real distributed effects. Verify Apache's rejection policy for unknown topic configuration using the same existing policy-aware test that covers Redpanda's ignore behavior.

M3 records the failure timeline, observed leaders/group phases, exact records/offsets, bounded durations, error/callback traces and cleanup. Every stopped owned broker must be restored or its entire owned fixture removed in cleanup. Integrate the accepted results into `docs/testing/compatibility-matrix.json` and public-contract links without claiming security until EP-19 proves it.

## Concrete Steps

Run existing baseline commands from the repository root, inside the primary shell or the selected actual environment:

```bash
nix develop
cabal build all -f examples -f integration
cabal test rdkafka-hs-test rdkafka-hs-integration -f integration --test-show-details=direct --test-options='--quickcheck-tests=1000 --quickcheck-replay=20261024'
cabal check
```

Use exact named Tasty selections during implementation, then the required complete candidate checks. A zero-test selection, compilation failure or skipped environment is not passing behavioral evidence. Keep ordinary tests in Cabal/Tasty and extend `scripts/test-mutations.hs` for semantic sensitivity checks; preserve its existing families. Temporary editing/probe scripts may be used freely, but minimize permanent runners.

Implement test modules such as `test-integration/Kafka/Integration/BrokerFailureTest.hs` and `RebalanceContractTest.hs`, register them explicitly, and keep orchestration scoped to their run-owned topology. The fixture must expose a real broker/leader control plus observed reachability/election barriers; a sleep alone does not prove the transition.

```bash
cabal build rdkafka-hs-integration -f integration
cabal test rdkafka-hs-integration -f integration --test-show-details=direct --test-options='-p /broker-failure/'
cabal test rdkafka-hs-integration -f integration --test-show-details=direct --test-options='-p /rebalance-contract/'
```

These group names are planned interfaces to implement. Before running them, the fixture must be available and the selection must list nonzero expected cases. Run each failure scenario with restoration/cancellation checks, then a healthy control. Record actual broker versions and source fingerprint.

## Validation and Acceptance

Actual Apache leader failover, bounded outage/reconnection, commit/poll recovery, eager/cooperative handoff, assignmentLost True at the supported window, transaction recovery/fencing and read-committed visibility have exact source-bound observations. No unexplained record loss, incorrect success, resource retention or borrowed-fixture mutation remains. Required applicable callback modes run, and unknown-topic configuration follows Apache's actual declared policy. An unavailable cluster blocks this child; controlled native frames cannot close its real-broker requirements.

## Idempotence and Recovery

Each run uses a unique prefix and restores only resources it owns. Close/destroy clients before deleting their topics or closing borrowed descriptors. Install cleanup before faulting a broker; use `finally` to restore it even if assertions or cancellation fail. Keep bounded waits and preserve exact errors, seeds, operation traces and logs when a test fails. Retry only after diagnosing the failed precondition or defect; a green rerun does not erase the failure. Never delete another run's topics, reset a shared cluster, alter machine-global 9092, purchase infrastructure, publish, tag or push. Preserve the unrelated `.seihou/manifest.json` edit.

## Interfaces and Dependencies

Hard prerequisite is the accepted primary candidate. Own real fault/group/transaction scenario modules and run-owned outage orchestration. Coordinate shared fixture lifecycle and ports with EP-19; consume EP-17's tuple fields. EP-20 needs this entire child Complete. Native error lifetime changes remain compatible with existing error-object bracketing, callback ownership and manual sync buffer rules in ADR-15, ADR-19, ADR-20 and ADR-25.

Commits use Conventional Commits with `MasterPlan: docs/masterplans/3-complete-deferred-coverage-and-compatibility-validation.md`, `ExecPlan: docs/plans/18-verify-distributed-broker-failure-and-group-recovery.md` and `Intention: intention_01m5p37ej8e3h92gkf2wp9j6qa`. Preserve original provenance and append later author/reviewer entries through the plan skill scripts.
