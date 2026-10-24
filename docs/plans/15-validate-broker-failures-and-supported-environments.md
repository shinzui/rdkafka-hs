---
id: 15
slug: validate-broker-failures-and-supported-environments
title: "Validate broker failures and supported environments"
kind: exec-plan
created_at: 2026-10-24T04:50:25Z
intention: "intention_01m5p37ej8e3h92gkf2wp9j6qa"
master_plan: "docs/masterplans/2-fix-inherited-release-review-defects.md"
provenance:
  created_by:
    model: "gpt-6-astra"
    harness: "codex-cli"
    at: 2026-10-24T04:50:25Z
---

# Validate broker failures and supported environments


This ExecPlan is a living document. Keep its evidence, milestones, dependencies, and decisions current.


## Purpose / Big Picture


Prove the combined candidate behaves correctly under real broker failures and in explicitly tested environments. Release evidence will distinguish supported tested behavior from assumptions about other compilers, native libraries, brokers, and operating systems. Authentication/authorization and native-memory safety are exercised beyond simple setter return values.


## Progress


- [ ] M1: Run-owned fault fixtures prove outage/reconnection, rebalance, and transactional recovery against real brokers with no shared-broker mutation.
- [ ] M2: Real authentication/authorization, compatibility tuples, and native lifetime diagnostics pass with exact environment evidence.
- [ ] M3: The complete candidate passes the public-contract/ownership inventory, supported matrix, optimized/instrumented builds, and source-distribution replay with no unexplained gaps.

Not started. The checks described below are deliverables to implement, not existing evidence.


## Surprises & Discoveries


The existing suite uses a single local Redpanda and one primary GHC/librdkafka tuple. `setSaslCredentials` tests call setters on unauthenticated unreachable clients, which does not prove authentication or credential replacement. Consumer destroy tests use a silent TCP listener, useful for shutdown bounds but insufficient for real disconnect/reconnect and broker failover behavior.


## Decision Log


2026-10-24: Cover meaningful environment axes with representative declared tuples, not an unbounded Cartesian product. Mandatory rows cannot be waived because a machine or diagnostic is unavailable. Keep all destructive fault actions within fixtures owned by this run; no publication or infrastructure purchases are authorized by this plan.


## Outcomes & Retrospective


Not implemented. Record observed results and durable lessons at completion; update relevant ADRs and the parent registry.


## Context and Orientation


Use completed EP-8–EP-14 tests and fixtures. Add `test-integration/Kafka/Integration/BrokerFailureTest.hs`, `RebalanceContractTest.hs`, and `AuthenticationTest.hs` as needed, plus `scripts/test-compatibility.sh` and `docs/testing/compatibility-matrix.json`. Existing private-broker lifecycle lives in `nix/redpanda.nix`, `flake.module.nix`, and `justfile`; do not change the machine-global broker. This child may add test-only isolated configurations using available supported tooling or consume a supplied disposable endpoint. It must not silently turn `just test-it` into a cluster-reset command.

This repository is `rdkafka-hs`, a Haskell wrapper around librdkafka. The reviewed baseline is `9c41b3d`; planning commit `7646e69` added the six-defect repair initiative. `rdkafka-hs.cabal` lists the public modules and two test suites: `rdkafka-hs-test` has 14 baseline tests and `rdkafka-hs-integration` has 116. `test/Main.hs` and `test-integration/Main.hs` register cases; `test-integration/Kafka/TestEnv.hs` owns broker fixtures. Generated bindings come from `src/Kafka/Internal/RdKafka.chs`, with hand-written conversion and lifetime code alongside them.

Read [ADR-10](../adr/release-coverage-requires-behavior-and-ownership-evidence.md): coverage means evidence for each supported behavior and critical ownership branch, not a passing count. [ADR-5](../adr/integration-tests-run-against-a-project-local-redpanda.md) protects the shared broker: use the private endpoint at `127.0.0.1:39092`, run-prefixed resources, and only run-owned fault targets. [ADR-1](../adr/consumer-queue-and-teardown-ownership.md) distinguishes graceful close from destruction and requires one native deallocator. [ADR-4](../adr/toolchain-floor-is-ghc-9-12-and-librdkafka-2-15.md) retains the existing compiler/library floor; [ADR-2](../adr/release-validation-before-publication.md) requires exact-candidate validation before publication. Existing first-release platform/provisioning limits in [ADR-7](../adr/first-release-scope-and-distribution.md) describe the earlier release; the new compatibility plan must document its additional test environments without rewriting that release history.

Use Mori to locate dependency sources before relying on APIs. `mori://confluentinc/librdkafka` owns the native API. `mori://haskell/cabal` owns project-relative `cabal/doc/cabal-project-description-file.rst`, whose coverage section documents `--enable-coverage`. `mori://UnkindPartition/tasty/packages/tasty-quickcheck` supplies a property-test adapter; its source is under project-relative `tasty/quickcheck/Test/Tasty/QuickCheck.hs` in `mori://UnkindPartition/tasty`. Source-file artifact URIs are pending. Consult the local source first; verify current releases through authoritative package registries/upstream tags before choosing new test dependency bounds. No runtime dependency upgrade is implied. Do not inspect `/nix/store` or filesystem root.


## Plan of Work


M1 builds deterministic failure fixtures using a run-owned proxy or isolated broker instances. Exercise connection refusal, an established connection that stops responding, connection loss during produce/poll/commit, broker restart, and recovery within configured timeouts. Verify callback counts and actual offsets/records: duplicates permitted by the chosen delivery mode must be distinguished from loss, and exactly-once claims require transactional proof. Never claim exactly-once delivery from ordinary producer retries. Use two consumers with unique group IDs to verify assignment, revocation, assignmentLost when supported, eager and cooperative strategies actually exposed through properties, and delivery across rebalance in both callback modes. A multi-broker Apache Kafka fixture must exercise leader failover for a replicated topic, with before/after leader metadata and record/commit observations. Broker restart and leader failover are different cases. Own and restore all processes, proxies, topics, and users this run mutates; add preflight checks that refuse shared or unowned endpoints for fault actions.

Reuse EP-12's transaction flow under interruption: source offsets advance with committed output and do not advance on abort, retryable failures recover, and fatal fencing requires a new producer. Use barriers/events or observed protocol state to trigger each failure window; arbitrary sleeps are not evidence the window was exercised. Assertion logs record the state reached before disruption, operation result, cleanup, and recovered state. Missing capabilities must fail the named mandatory case instead of silently skipping it.

M2 defines concrete released environment tuples in `docs/testing/compatibility-matrix.json`. Required axes are the current Darwin/Redpanda/GHC 9.12.4/librdkafka 2.15 baseline; Linux build/unit plus representative real-broker integration and native invalid-access/leak diagnostics; an Apache Kafka broker configuration including replicated failover; and a newer released compiler/native-library tuple compatible with declared package bounds. A tuple may cover multiple axes. If the floor is already the newest compatible release, document registry/tag evidence and collapse that axis rather than inventing a version. Record exact OS/architecture, GHC, Cabal, c2hs, librdkafka, broker image digest/configuration, commands, and source identity for each tuple. Test the library version actually loaded, not only headers used to compile. The entire deterministic baseline suite runs on the primary tuple, and cross-tuple smoke selections must include F1–F6, both poll modes, core produce/consume/offset/transaction flows, and C/Haskell lifetime tests. A blank or unrun required tuple blocks completion. Do not silently narrow advertised support to make a matrix green; surface that as a separate scope decision.

Add a private broker security configuration and test successful TLS/authentication, rejected credentials/certificate verification, authorization denial for production/consumption/admin operations, and `setSaslCredentials` affecting a subsequent authentication after a controlled reconnect. Preserve existing authenticated-connection semantics: the setter does not promise immediate reauthentication. Use ephemeral credentials/certificates and scrub secrets from evidence. Exercise supported SASL mechanisms promised in project documentation; unclaimed native-library mechanisms need an explicit applicability rationale rather than assumed coverage. Do not infer security coverage from a plaintext setter test.

Run allocation-attributed/native invalid-access diagnostics on the relevant C ownership tests. Linux sanitizer or Valgrind evidence and Darwin leak/weak-reference evidence are complementary; verify the selected tool can see librdkafka and Haskell/C bridge allocations using a deliberately failing control. If a diagnostic cannot instrument a component, document the limitation and supply an effective alternative for that required path; no clean-looking but uninstrumented result counts. Keep suppression files specific and justified, and never suppress project allocation stacks or ignore a positive control failure.

M3 runs the source-distribution build/tests on a clean extraction and replays the evidence manifest against the final candidate. Implement `just test-compatibility` as a fail-closed entry point: it preflights required fixtures, runs or verifies current-revision tuple evidence, and reports every unavailable/failed/stale row. Existing local development tests remain convenient, while the release gate requires the complete matrix. Re-run changed scenarios when source/configuration changes invalidate their proof. Update the testing guide, README's tested-environment claims, and the relevant ADRs for the accepted execution environment and lifetime diagnostics. Distinguish borrowed external test endpoints from local broker orchestration; this initiative is not a general deployment platform or a CI rollout.


## Concrete Steps


Run from `/Users/shinzui/Keikaku/bokuno/rdkafka-hs`, or the current checkout root:

```bash
nix develop
mori registry show confluentinc/librdkafka --full
mori registry show haskell/cabal --full
mori registry show UnkindPartition/tasty --full
cabal build all -f examples -f integration
cabal test rdkafka-hs-test --test-show-details=direct
just test-it
```

Run focused cases while developing and the full required gates after meaningful changes. Preserve unrelated `.seihou/manifest.json` edits. Source changes require normal and instrumented validation; documentation-only evidence updates do not justify repeating all expensive scenarios. Commit implementation boundaries using Conventional Commits plus this child's ExecPlan trailer, `MasterPlan: docs/masterplans/2-fix-inherited-release-review-defects.md`, and `Intention: intention_01m5p37ej8e3h92gkf2wp9j6qa`.

The primary suite and final gate run from the development shell:

```bash
just test-it
just coverage
just test-compatibility
just coverage-check
cabal check
cabal sdist
```

EP-15 must add the third command and exact per-tuple invocation recipes to `docs/testing/coverage.md`. Test `just test-compatibility` with an intentionally missing fixture and stale evidence to prove nonzero exit. Use existing `RDKAFKA_TEST_BROKER` for an explicitly supplied private endpoint only after ownership/capability preflight; never substitute port 9092. Record actual diagnostic/tool arguments, no placeholder commands, before acceptance.


## Validation and Acceptance


Every required fault scenario observes the targeted state before disruption and asserts record/error/offset outcomes afterward; reconnection and leader failover both have evidence. Both callback modes preserve their defined behavior through applicable failures. Real authentication succeeds and fails as expected, and credential rotation is verified on a fresh authentication. All required environment axes have source-bound evidence; unavailable tooling/endpoints produce an incomplete status. Native diagnostics catch their intentionally faulty controls and report no unexplained project leaks/invalid accesses in the repaired candidate. Source-distribution tests reproduce supported behavior. `just coverage-check` passes with zero open or unexplained supported-contract/critical-ownership rows, and `just test-compatibility` passes without skip or allow-open flags. This does not claim all possible inputs, thread schedules, or dependency versions were exhaustively tested; it establishes an explicit checked support contract.


## Idempotence and Recovery


All runs must be repeatable with fresh run IDs. Store generated reports under ignored build/artifact directories; commit the inventory schema, requirements, concise evidence manifest, and commands, not large generated HTML. Do not remove another run's topics or stop a broker not owned by the fixture. On failure retain the seed, operation trace, exact environment, and failure log; restore owned fault fixtures in `finally` before reporting failure. A retry that passes does not erase an earlier failure: diagnose it or keep the row open. Unavailable required environments remain blockers. No tagging, upload, remote publication, or downstream repository mutation is part of this plan.


## Interfaces and Dependencies


Hard dependencies on whole children `docs/plans/8-apply-topic-creation-configuration-safely.md`, `docs/plans/9-honor-producer-settings-and-release-rejected-callbacks.md`, `docs/plans/10-report-per-partition-seek-failures.md`, `docs/plans/11-own-native-callbacks-through-client-teardown.md`, `docs/plans/12-release-transaction-error-objects.md`, `docs/plans/13-make-coverage-evidence-and-test-fixtures-reliable.md`, and `docs/plans/14-cover-the-public-api-and-pure-data-laws.md`. This ensures expensive cross-environment/fault tests exercise a locally repaired and verified candidate. EP-13 owns inventory schema and base fixtures; this child owns environment capability/evidence schema and fault/security fixtures. The master coordinator reconciles final evidence and marks the initiative complete; publication remains separate.
