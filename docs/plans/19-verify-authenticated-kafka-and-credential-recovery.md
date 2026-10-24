---
id: 19
slug: verify-authenticated-kafka-and-credential-recovery
title: "Verify authenticated Kafka and credential recovery"
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

# Verify authenticated Kafka and credential recovery

## Purpose / Big Picture

Users can rely on demonstrated authenticated connections, TLS validation and authorization failures, including replacement credentials taking effect on a subsequent authentication. Merely calling setSaslCredentials on a plaintext/unreachable client does not prove this behavior.

## Progress

- [ ] M1: Provision isolated ephemeral security resources and prove a real authenticated produce/consume plus an actual denied operation.
- [ ] M2: Verify invalid/valid credentials, TLS failures, ACL effects and setter-driven authentication after controlled reconnect for producer and consumer.
- [ ] M3: Accept scrubbed, source-bound security evidence and restored credentials/certificates/ACLs for EP-20.

Not Started. This plan preserves deferred requirements and existing evidence; no implementation result is implied.

## Surprises & Discoveries

Both public setSaslCredentials functions currently have setter/error-object checks, but those clients are not authenticated and no credential replacement was proven at a broker. The native setter affects future authentications; it does not promise immediate reauthentication of existing connections. Preserve that semantics. No real TLS, authorization or reconnect success may be inferred from the existing plaintext setter cases.

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

M1 checks the actual documented SASL mechanisms and TLS settings exposed through this package's property maps. Build an isolated Apache Kafka security fixture with ephemeral users, CA/server certificates and separate allowed/denied identities, reusing EP-18's topology if available. Do not edit shared cluster users or ACLs. Select at least one real username/password SASL mechanism supported by setSaslCredentials and run it with TLS; inventory other mechanisms promised in project documentation and test each or record a concrete applicability rationale for mechanisms never claimed by this package. OAuth/Kerberos or a general identity service is not silently added as a new capability. Verify the released broker/native configuration against source/docs. Prove one real authenticated producer delivery and consumer record, and a real denied operation, before expanding credential variants.

M2 exercises successful and invalid authentication for both producer and consumer, exact reported errors and cleanup after rejected construction/operation where applicable. TLS cases include trusted certificate/hostname success and untrusted or mismatched identity failure; success cannot disable verification. Authorization cases deny produce, consume/group access and administration as applicable, then verify allowed identities perform the exact operation. Match broker/native error categories and callback behavior rather than accepting an arbitrary Left.

For credential replacement, first establish a successful authenticated connection with an ephemeral identity. Call setSaslCredentials with a second valid identity/password and deliberately force only the fixture-owned connection to reconnect. Observe a new authentication and verify a record/ACL action that distinguishes the new credentials from the old. Include invalid replacement credentials followed by corrected credentials and recovery. The setter need not disconnect a currently authenticated connection; test that documented boundary rather than demanding immediate revocation. Test producer and consumer, including supported callback modes, with exact delivery/record/commit observations and bounded cleanup. Verify non-null setter error objects are released, retaining existing counted mutation controls.

M3 records the mechanism, TLS/ACL policy, sanitized identity labels, broker/native tuple, source fingerprint, commands and exact non-secret outcomes in the compatibility matrix and `docs/testing/authentication-evidence.json`. No password, key, certificate private material, token or connection URL containing secrets enters source control, logs or mutation archives. Evidence describes cleanup of owned users/ACLs/certificates and verifies no owned resources remain after interruption.

## Concrete Steps

Run existing baseline commands from the repository root, inside the primary shell or the selected actual environment:

```bash
nix develop
cabal build all -f examples -f integration
cabal test rdkafka-hs-test rdkafka-hs-integration -f integration --test-show-details=direct --test-options='--quickcheck-tests=1000 --quickcheck-replay=20261024'
cabal check
```

Use exact named Tasty selections during implementation, then the required complete candidate checks. A zero-test selection, compilation failure or skipped environment is not passing behavioral evidence. Keep ordinary tests in Cabal/Tasty and extend `scripts/test-mutations.hs` for semantic sensitivity checks; preserve its existing families. Temporary editing/probe scripts may be used freely, but minimize permanent runners.

Add/register `test-integration/Kafka/Integration/AuthenticationTest.hs` and a fixture interface supplying temporary secret paths/environment only to its child process. Never print that environment wholesale. Planned test group:

```bash
cabal build rdkafka-hs-integration -f integration
cabal test rdkafka-hs-integration -f integration --test-show-details=direct --test-options='-p /authentication-contract/'
```

Make missing credentials/secure fixture a preflight failure for this qualification run. Confirm the expected nonzero test list, actual authenticated broker identity and required TLS verification before accepting results. Extend the existing Haskell mutation runner only for a concrete uncovered semantic sensitivity; do not create a separate permanent Python security runner.

## Validation and Acceptance

An actual broker proves valid and invalid authentication, TLS validation successes/failures, authorization allow/deny effects, and credential replacement on a subsequent observed reconnect for both client kinds. Exact errors, records and callback outcomes are asserted; cleanup is effective and secrets are absent from committed/logged evidence. Supported mechanisms have explicit tested/applicability status. A plaintext setter smoke, successful constructor or mocked error does not close this child.

## Idempotence and Recovery

Each run uses a unique prefix and restores only resources it owns. Close/destroy clients before deleting their topics or closing borrowed descriptors. Install cleanup before faulting a broker; use `finally` to restore it even if assertions or cancellation fail. Keep bounded waits and preserve exact errors, seeds, operation traces and logs when a test fails. Retry only after diagnosing the failed precondition or defect; a green rerun does not erase the failure. Never delete another run's topics, reset a shared cluster, alter machine-global 9092, purchase infrastructure, publish, tag or push. Preserve the unrelated `.seihou/manifest.json` edit.

## Interfaces and Dependencies

Hard prerequisite is the accepted primary candidate. Own secure broker configuration and ephemeral credentials/certificates/ACL lifecycle; EP-18 can supply shared topology but is a soft dependency. Integrate with EP-17's tuple schema and EP-20's complete gate. Preserve native error-object destruction and the public setter result types. New authentication mechanisms or credential management products are outside this plan.

Commits use Conventional Commits with `MasterPlan: docs/masterplans/3-complete-deferred-coverage-and-compatibility-validation.md`, `ExecPlan: docs/plans/19-verify-authenticated-kafka-and-credential-recovery.md` and `Intention: intention_01m5p37ej8e3h92gkf2wp9j6qa`. Preserve original provenance and append later author/reviewer entries through the plan skill scripts.
