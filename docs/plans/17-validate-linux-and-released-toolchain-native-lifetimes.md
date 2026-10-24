---
id: 17
slug: validate-linux-and-released-toolchain-native-lifetimes
title: "Validate Linux and released toolchain native lifetimes"
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

# Validate Linux and released toolchain native lifetimes

## Purpose / Big Picture

Users gain execution evidence for Linux and a newer compatible released toolchain/native combination, together with memory diagnostics proven capable of observing the native and Haskell/C bridge allocations they claim to cover. A cross-build or an installed-but-unloaded sanitizer is not this result.

## Progress

- [ ] M1: Define source-bound tuple evidence and pass an actual Linux build plus representative broker/runtime smoke.
- [ ] M2: Demonstrate effective native leak and invalid-access controls, then pass candidate lifetime scenarios on Linux and complementary Darwin diagnostics.
- [ ] M3: Execute a registry/tag-verified newer released compiler/native tuple and publish accurate compatibility results for EP-20.

Not Started. This plan preserves deferred requirements and existing evidence; no implementation result is implied.

## Surprises & Discoveries

Existing Darwin native AddressSanitizer runs use librdkafka 2.15.0 and `detect_leaks=0`; they establish invalid-access coverage only for the named scenarios. `docs/testing/callback-native-diagnostics.json` contains an effective deliberate invalid-access control. Existing weak-reference tests establish collection of Haskell captures, not all native allocations. These proofs remain valuable and must not be replaced with an unsupported claim of whole-process leak freedom.

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

M1 inventories available execution resources without buying infrastructure. Choose an actual Linux host or isolated container/VM where the Haskell executable and native client really execute. Record OS/architecture, compiler, Cabal, native build/loading, broker identity/topology, test environment, source fingerprint and command/artifact hashes in `docs/testing/compatibility-matrix.json`. Preserve the primary row and distinguish passed, failed, unavailable and not-run states. Agree these fields with EP-18 and EP-19 through one real producer/consumer and counted callback-lifetime check before expanding variants. Build the library, examples and suites on Linux and run representative producer delivery, header order, seek, callback teardown, transaction-error and partition/message ownership cases against a run-owned or explicitly supplied private endpoint. Exercise actual threaded/nonthreaded runtime variants where supported.

M2 chooses an effective Linux diagnostic such as sanitizer-enabled native code or Valgrind, documenting whether it observes GHC/C bridge allocations and librdkafka internal allocations. Demonstrate a deliberate retained native resource and an invalid-access mutation are detected in the relevant allocation stacks. Reuse existing barriers, native counters, retained-resource controls and weak references. Required paths include producer success and immediate rejection, queue-full/oversize, accepted callback lifetime, cancellation after enqueue, queued/in-flight purge, native headers created inside produceva, persistent callbacks through native shutdown, client/config failure cleanup, batch/single messages, metadata/group arrays, diagnostics and partition lists. Never equate freeing borrowed Haskell buffers with releasing native-owned header objects. Run the candidate without unexplained leaks or invalid accesses in those paths; use specific source-backed suppressions only for unrelated runtime allocations, never to suppress project stacks or a failing positive control. Complement this with Darwin allocation/weak-reference evidence where Linux cannot establish a Darwin-specific lifetime.

M3 discovers released versions through Mori, authoritative package registries and upstream tags. Select a newer compatible compiler/native tuple rather than an arbitrary development checkout; record the version decision and compatibility constraints. Preserve existing runtime pins and avoid seihou-managed scaffolding changes solely to add another toolchain. Execute an actual build and representative broker/native-lifetime cases on the selected tuple, and document the loaded library. Test the same topic-configuration policy case under each broker declared by the final matrix, consuming EP-18's Apache result where relevant. Update tested claims without expanding bounds or platform guarantees on the basis of compilation alone.

## Concrete Steps

Run existing baseline commands from the repository root, inside the primary shell or the selected actual environment:

```bash
nix develop
cabal build all -f examples -f integration
cabal test rdkafka-hs-test rdkafka-hs-integration -f integration --test-show-details=direct --test-options='--quickcheck-tests=1000 --quickcheck-replay=20261024'
cabal check
```

Use exact named Tasty selections during implementation, then the required complete candidate checks. A zero-test selection, compilation failure or skipped environment is not passing behavioral evidence. Keep ordinary tests in Cabal/Tasty and extend `scripts/test-mutations.hs` for semantic sensitivity checks; preserve its existing families. Temporary editing/probe scripts may be used freely, but minimize permanent runners.

On the selected Linux/newer execution host, record commands and actual output before the tests:

```bash
uname -a
ghc --numeric-version
cabal --numeric-version
pkg-config --modversion rdkafka
cabal build all -f examples -f integration
cabal test rdkafka-hs-test rdkafka-hs-integration -f integration --test-show-details=direct
```

Configure `RDKAFKA_TEST_BROKER` explicitly to a private supplied/run-owned fixture. Reuse the existing mutation families and native control entry points documented in the ownership evidence; record the exact diagnostic command, loaded native identity, failing control and clean candidate result. Future paths or wrapper commands are implementation outputs, not existing proof.

## Validation and Acceptance

There is actual Linux runtime evidence, an actual newer released tuple, accurate primary tuple retention, effective leak/invalid-access controls for the named ownership paths, and no unexplained candidate diagnostic finding. Every matrix row records exact source/configuration and loaded versions. Unavailable required hosts/tools remain blockers for comprehensive qualification, not successful skips. Native internal header lifetime has a detected intentional cleanup failure and a clean candidate result.

## Idempotence and Recovery

Each run uses a unique prefix and restores only resources it owns. Close/destroy clients before deleting their topics or closing borrowed descriptors. Install cleanup before faulting a broker; use `finally` to restore it even if assertions or cancellation fail. Keep bounded waits and preserve exact errors, seeds, operation traces and logs when a test fails. Retry only after diagnosing the failed precondition or defect; a green rerun does not erase the failure. Never delete another run's topics, reset a shared cluster, alter machine-global 9092, purchase infrastructure, publish, tag or push. Preserve the unrelated `.seihou/manifest.json` edit.

## Interfaces and Dependencies

Hard prerequisite is MasterPlan 2's accepted primary candidate. Own shared tuple fields and native/toolchain diagnostics; EP-18/19 consume/extend the matrix without redefining fields. EP-18 supplies Apache topology behavior; EP-20 requires this whole child Complete. Preserve `docs/testing/coverage-classification-remap.json` and existing lifetime manifests as historical source-bound evidence. Additional runtime dependency bounds are outside scope unless an observed compatibility defect requires a separately justified decision.

Commits use Conventional Commits with `MasterPlan: docs/masterplans/3-complete-deferred-coverage-and-compatibility-validation.md`, `ExecPlan: docs/plans/17-validate-linux-and-released-toolchain-native-lifetimes.md` and `Intention: intention_01m5p37ej8e3h92gkf2wp9j6qa`. Preserve original provenance and append later author/reviewer entries through the plan skill scripts.
