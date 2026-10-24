---
id: 15
slug: validate-broker-failures-and-supported-environments
title: "Validate the primary release candidate"
kind: exec-plan
created_at: 2026-10-24T04:50:25Z
intention: "intention_01m5p37ej8e3h92gkf2wp9j6qa"
master_plan: "docs/masterplans/2-fix-inherited-release-review-defects.md"
provenance:
  created_by:
    model: "gpt-6-astra"
    harness: "codex-cli"
    at: 2026-10-24T04:50:25Z
  revisions:
    - model: "gpt-6.1-sol"
      harness: "codex-cli"
      at: 2026-10-24T21:49:43Z
      mode: "implement"
      note: "Keep real assignment-loss True evidence with the existing EP-15 eviction and handoff scenarios."
---

# Validate the primary release candidate

## Purpose / Big Picture

Finish a release-ready candidate on the demonstrated primary environment, with clean package execution and truthful limits. On 2026-10-24 the user explicitly narrowed this child and EP-14, requested a comprehensive follow-up and required preserving all started work. [ADR-27](../adr/release-validation-has-explicit-primary-and-expanded-scopes.md) defines this scope. The broader former requirements are preserved in [MasterPlan 3](../masterplans/3-complete-deferred-coverage-and-compatibility-validation.md), EP-17 through EP-20, and [the deferred ledger](../testing/deferred-validation.md).

## Progress

- [x] M1: The exact primary compiler/native/broker/runtime tuple and complete normal/mapped suite are source-bound, with retained semantic/tooling/native controls and truthful diagnostic limits.
- [x] M2: A clean source distribution builds all enabled components and passes its unit suite, representative repair/ownership broker cases and bounded example verification.
- [x] M3: Required Cabal/binding/documentation/Nix checks and primary inventory freshness pass; evidence, tested claims and the separate release handoff agree.

Complete under the user-selected primary scope. The clean source package matches all 103 source/test/example/script files, builds the library/example/both test suites, passes all 78 unit cases plus 23 ownership/runtime and eight six-repair cases, and independently verifies the bounded actual example. Required Cabal/binding/documentation/Nix checks and the primary inventory check pass. See `docs/testing/primary-validation-evidence.json` and `docs/testing/compatibility-matrix.json`.

The frozen source fingerprint `060dade4e8a9c764dabee54df3030fa6d857be3ea1404a2bc680a45ddadd5e4e` passes all 78 unit and 708 integration cases normally and mapped, plus four actual nonthreaded runtime checks. Twenty-four tooling controls pass. The primary environment is macOS 26.7, Darwin 25.6.0/arm64, GHC 9.12.4, Cabal 3.16.1.0, loaded librdkafka 2.15.0 and private Redpanda v26.2.1. Package replay and final checks are accepted.

## Surprises & Discoveries

The original wider acceptance had no implementation started in this child. Existing work from EP-8 through EP-14 includes real primary broker and controlled native failure/ownership cases, generated laws, semantic mutation families and focused AddressSanitizer runs. Preserve them. They do not establish Linux, Apache multi-broker, real authenticated TLS/ACL operation or native leak freedom. Current Darwin ASan runs use leak detection disabled; weak references and counters have their own narrower claims.

The full report leaves 192 unclassified handwritten ticks and 35 historical open inventory rows, many now backed by tests awaiting exhaustive reconciliation. The user transferred these explicitly; `docs/testing/deferred-validation-snapshot.json` preserves exact rows, source keys and evidence hashes without changing their status to passing.

## Decision Log

2026-10-24: The user selected primary-environment release qualification and a comprehensive follow-up. Replace this child's unstarted broad matrix with concrete primary package/candidate acceptance. Retain the original broader requirements in EP-17 (Linux/newer/native lifetime diagnostics), EP-18 (Apache distributed failures/groups/transactions), EP-19 (real security/reconnect), and EP-20 (strict complete gate). No test or implemented repair is removed.

2026-10-24: Reuse the full identical-source normal/mapped runs for the checkout. Clean package acceptance builds every enabled component, verifies packaged source identity, runs all unit cases and representative nonzero repair/ownership/runtime broker cases plus the bounded actual example. Repeating the whole identical broker suite in every extraction is unnecessary unless package differences or failures invalidate the source proof.

## Outcomes & Retrospective

Primary execution, clean package replay and the final primary gate are accepted. All original fixes/tests and the complete mapped evidence remain retained. Completing this child prepares a separate release task and does not publish, choose a new package version, tag, push or mark the broader intention complete. Comprehensive qualification remains tracked under MasterPlan 3.

## Context and Orientation

`rdkafka-hs.cabal` registers the library, example, unit suite and integration suite. `cabal sdist` must include every registered Haskell/C test source and the existing test/coverage tools. `test-integration/Kafka/TestEnv.hs` owns private broker fixtures and strict assertions. The current private broker uses 127.0.0.1:39092/admin 39644; no action may mutate the machine-global broker on 9092.

`docs/testing/coverage-evidence.json` preserves the current full mapped candidate, loaded version, runtime supplement and raw artifact hashes. Focused manifests such as `partition-list-evidence.json`, `final-boundaries-evidence.json`, `legacy-seek-evidence.json`, `callback-native-diagnostics.json`, and the earlier repair evidence retain native/semantic controls. `scripts/test-mutations.hs` is the single Haskell mutation runner; do not recreate the four replaced Python runners. Python remains allowed for edits/probes; minimize permanent scripts.

[ADR-2](../adr/release-validation-before-publication.md) requires exact-candidate validation before separate publication. [ADR-5](../adr/integration-tests-run-against-a-project-local-redpanda.md) protects shared resources. [ADR-27](../adr/release-validation-has-explicit-primary-and-expanded-scopes.md) governs this explicit release scope, while [ADR-10](../adr/release-coverage-requires-behavior-and-ownership-evidence.md) retains comprehensive qualification under MasterPlan 3. Existing distribution history in ADR-7 stays historical fact.

Use Mori before dependency APIs and authoritative registries/upstream tags before any new version/bound decision. No new tuple or bound is needed for this primary replay. Never traverse `/nix/store` or filesystem root.

## Plan of Work

M1 accepts the complete primary source-bound normal/mapped runs, actual nonthreaded supplement, existing semantic/native controls and tooling checks. Record actual OS/compiler/Cabal/native/broker identities, not only build-time flags. Keep all diagnostic limitations visible. Update only stale existing inventory references/fingerprints supported by the complete current run; moved test groups keep the same cases. Do not mislabel the exhaustive deferred rows as tested.

M2 creates a source tarball under an ignored build directory, records its hash, checks that all registered source/test/C/script files are included, and extracts into a fresh directory with no source symlinks back to the checkout. Build library, examples and both suites there using the same primary environment. Compare packaged source/test bytes to the checkout. Run all unit properties with 1,000 cases/seed 20261024 and a representative broker selection that exercises topic configuration, producer ownership, seek errors, persistent callback teardown, transaction errors, partition-list cleanup and runtime behavior. Fail on missing groups or zero tests. Run the actual example executable with broker/topic/group arguments under a timeout and independently verify its exact records/offsets/headers/committed offset, reusing the existing temporary Haskell example verifier where available. Retain outputs and exact commands.

M3 runs Cabal/binding/documentation/Nix checks and the explicit `scripts/check-coverage.py --allow-open` source-freshness/inventory validation. `just coverage-check` remains strict and is not this primary release gate; future `just test-compatibility` belongs to EP-20. Record one accurate primary row in `docs/testing/compatibility-matrix.json` and a concise `docs/testing/primary-validation-evidence.json`. Link every deferred obligation to MasterPlan 3. Update README/testing claims, complete this child and MasterPlan 2's primary integration gate, and leave a concrete separate release handoff. No publication is authorized here.

## Concrete Steps

From the repository root:

```bash
nix develop -c cabal build all -f examples -f integration
nix develop -c cabal check
nix develop -c cabal sdist --output-directory=dist-primary-release
nix develop -c python3 scripts/check-coverage.py --allow-open
nix develop -c just check
```

The complete normal and mapped source commands are already captured in the current full evidence; rerun only if source/configuration changes invalidate it. In the fresh tarball extraction, run:

```bash
cabal build all -f examples -f integration
cabal test rdkafka-hs-test --test-show-details=direct --test-options='--quickcheck-tests=1000 --quickcheck-replay=20261024'
cabal test rdkafka-hs-integration -f integration --test-show-details=direct --test-options='-p /topic-configuration/||/partition-list-ownership/||/runtime-contract/||/seek-partitions/'
```

Supplement that named selection with the existing producer/callback/transaction repair groups and document all actual names/counts. The example verification uses three stdin lines and checks four exact records, the final fancy header and broker committed next offset four; see `docs/testing/example-evidence.json`. These are required behaviors, not an instruction to install another permanent runner.

## Validation and Acceptance

Every implemented regression passes; full primary normal/mapped source evidence and the actual runtime supplement are current. Clean package source identity, enabled-component builds, complete unit cases, representative repair/ownership/runtime broker cases and independent bounded example effects pass. Required package/binding/documentation/Nix checks pass. The primary report lists exact versions and accurate native instrumentation limits. The 35 historical open rows and 192 current unclassified ticks remain explicitly preserved for the comprehensive follow-up; no confirmed failing regression is deferred or hidden. Actual release version selection/publication is separate.

## Idempotence and Recovery

Use a fresh ignored extraction and unique topic/group prefixes. Do not reuse a dirty package build as a clean replay. Restore only owned fixture resources in `finally`; keep descriptors open through native consumer destruction. Preserve failed seeds/logs and diagnose a failure before accepting a retry. Do not delete shared topics, reset clusters, alter 9092, touch unrelated `.seihou/manifest.json`, move tags, upload or push. Documentation-only changes do not require repeating identical-source full broker execution.

## Interfaces and Dependencies

Whole-child hard dependencies are EP-8 through EP-14 under `docs/masterplans/2-fix-inherited-release-review-defects.md`; EP-14 is Complete under the user-approved primary scope. Own the primary compatibility row, package replay and final handoff. MasterPlan 3 owns exhaustive/global qualification and its separate gate; it does not block this explicitly narrowed release. Preserve all original artifact/source links and provenance. Commits include this ExecPlan, the current MasterPlan and intention trailers.
