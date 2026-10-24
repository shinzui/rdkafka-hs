---
id: 14
slug: cover-the-public-api-and-pure-data-laws
title: "Cover the public API and pure-data laws"
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
      at: 2026-10-24T08:48:23Z
      mode: "implement"
      note: "Integrate the shared property adapter and generated producer composition milestone before closing EP-9; retain all other public-contract work as open."
---

# Cover the public API and pure-data laws


This ExecPlan is a living document. Keep its evidence, milestones, dependencies, and decisions current.


## Purpose / Big Picture


Close the supported public-API gaps that remain beyond the six repair findings. Exact broker results, generated-data laws, and meaningful failure cases replace the assumption that an exercised function is a tested contract. Every remaining inventory row gets a real test or a source-backed not-applicable rationale.


## Progress


- [ ] M1: Generated-data properties verify public composition, conversion, record, header, and version laws with replayable failures.
- [ ] M2: Consumer, metadata, diagnostics, and deprecated public entry points have exact positive/negative contract tests.
- [ ] M3: All public-contract rows owned by this child pass normal and instrumented runs and survive deliberate fault/mutation checks.

In Progress. The scheduled M1 producer-composition integration milestone is accepted, allowing EP-9 to complete. Shared generators and replay support live in `test-support/Kafka/PropertySupport.hs`; candidate identity includes that directory and an added tooling control detects its modification. Six pure laws plus a fixed edge case and a native callback-composition property pass. Four semantic bias/order mutations compile, shrink to small counterexamples and reproduce with the printed replay tokens. Seeded stress passes 1,000 cases for each pure law and 1,000 native callback programs.

The current complete suites pass 21 unit and 244 integration tests in normal and instrumented builds, plus 18 tooling controls and Nix/Cabal/ADR/binding checks. See [property evidence](../testing/producer-properties-evidence.json). Remaining consumer/subscription/header/record/version/conversion laws, public API state/error/diagnostic coverage, the saved deprecated-seek failure and all remaining M2/M3 requirements stay open. Resume them after EP-10 through EP-12 under the master schedule.


## Surprises & Discoveries

EP-11 also observed that cooperative rebalance handlers discard the owned error pointers from `rdKafkaIncrementalAssign` / `rdKafkaIncrementalUnassign`. Reproduce the native error branches with counted error objects and repair their disposal/observable failure handling under this child's remaining consumer-state/error contract. Ordinary successful eager rebalances do not exercise those branches.

EP-11's native reference audit found a batch ownership window: `rdKafkaConsumeBatchQueue` returns raw messages without finalizers, and `pollMessageBatch` traverses `fromMessagePtr`. That converter destroys its current message in `finally`, but interruption or conversion failure can leave later messages unprocessed and unowned. Reproduce with a counted native boundary and repair the batch's complete ownership scope under this child's polling/batch acceptance. Do not close this discovery through a success-only batch run.


The original unit suite covers four consumer-property examples, seven record mapping/traversal examples, and three version examples. There are no generated property suites. The metadata integration scenario often checks only Right/length, and deprecated seek is covered more thoroughly than its replacement. Current producer and consumer Type modules expose constructors, so the inventory must use actual exports rather than Haddock prose saying a constructor is hidden.


The local dependency corpus has unreleased adapter 0.12 metadata. The current Hackage preferred registry and upstream `quickcheck-0.11.1` tag identify release 0.11.1; the released source was inspected at commit `f838314360d1c8f20f2919b376a5d2672aedc893`. Hackage revision 7 permits QuickCheck below 2.20. The executed Cabal plan resolves adapter 0.11.1, QuickCheck 2.19.0.0 and Tasty 1.5.5; runtime pins and existing Tasty bounds are unchanged.

The global-bias mutation shrinks to `[Global "shared" "", Global "shared" "a"]`; the callback-order mutation reduces to two distinguishable callbacks. The full replay tokens and outputs are retained in the property manifest. Functions are observed through native delivery rather than compared for equality.

## Decision Log


2026-10-24: Use test-only `^>=0.11.1` for [mori://UnkindPartition/tasty/packages/tasty-quickcheck](mori://UnkindPartition/tasty/packages/tasty-quickcheck), verified against the [preferred package registry](https://hackage-content.haskell.org/package/tasty-quickcheck/preferred.json), [current revised package description](https://hackage-content.haskell.org/package/tasty-quickcheck-0.11.1/tasty-quickcheck.cabal) and upstream release tag. Keep the shared adapter/producer milestone separate from the rest of M1 and the full child's acceptance.

2026-10-24: Test declared behavior with independent models and real state observations. Re-exports and deprecated functions remain obligations unless formally removed; generated repetitions of the same happy path do not close missing error branches. A newly found bug in an existing supported contract remains a repair obligation, not an expected-failure exemption.


## Outcomes & Retrospective


Not implemented. Record observed results and durable lessons at completion; update relevant ADRs and the parent registry.


## Context and Orientation


Use EP-13's inventory to enumerate exact symbols; candidate source modules include `src/Kafka/Types.hs`, `src/Kafka/Consumer/Types.hs`, `src/Kafka/Consumer/ConsumerProperties.hs`, `src/Kafka/Consumer/Subscription.hs`, `src/Kafka/Producer/ProducerProperties.hs`, `src/Kafka/Metadata.hs`, `src/Kafka/Dump.hs`, and `src/Kafka/Version.hs`, plus remaining functions in `src/Kafka/Consumer.hs`. Add focused unit modules under `test/Kafka` and integration modules such as `PublicConsumerContractTest.hs`, `MetadataContractTest.hs`, and `DiagnosticContractTest.hs`. Register every module explicitly. EP-8–EP-12 own their repaired behavior rows; reference those tests instead of duplicating ownership.

This repository is `rdkafka-hs`, a Haskell wrapper around librdkafka. The reviewed baseline is `9c41b3d`; planning commit `7646e69` added the six-defect repair initiative. `rdkafka-hs.cabal` lists the public modules and two test suites: `rdkafka-hs-test` has 14 baseline tests and `rdkafka-hs-integration` has 116. `test/Main.hs` and `test-integration/Main.hs` register cases; `test-integration/Kafka/TestEnv.hs` owns broker fixtures. Generated bindings come from `src/Kafka/Internal/RdKafka.chs`, with hand-written conversion and lifetime code alongside them.

Read [ADR-10](../adr/release-coverage-requires-behavior-and-ownership-evidence.md): coverage means evidence for each supported behavior and critical ownership branch, not a passing count. [ADR-5](../adr/integration-tests-run-against-a-project-local-redpanda.md) protects the shared broker: use the private endpoint at `127.0.0.1:39092`, run-prefixed resources, and only run-owned fault targets. [ADR-1](../adr/consumer-queue-and-teardown-ownership.md) distinguishes graceful close from destruction and requires one native deallocator. [ADR-4](../adr/toolchain-floor-is-ghc-9-12-and-librdkafka-2-15.md) retains the existing compiler/library floor; [ADR-2](../adr/release-validation-before-publication.md) requires exact-candidate validation before publication. Existing first-release platform/provisioning limits in [ADR-7](../adr/first-release-scope-and-distribution.md) describe the earlier release; the new compatibility plan must document its additional test environments without rewriting that release history.

Use Mori to locate dependency sources before relying on APIs. `mori://confluentinc/librdkafka` owns the native API. `mori://haskell/cabal` owns project-relative `cabal/doc/cabal-project-description-file.rst`, whose coverage section documents `--enable-coverage`. `mori://UnkindPartition/tasty/packages/tasty-quickcheck` supplies a property-test adapter; its source is under project-relative `tasty/quickcheck/Test/Tasty/QuickCheck.hs` in `mori://UnkindPartition/tasty`. Source-file artifact URIs are pending. Consult the local source first; verify current releases through authoritative package registries/upstream tags before choosing new test dependency bounds. No runtime dependency upgrade is implied. Do not inspect `/nix/store` or filesystem root.


## Plan of Work


M1 adds generated tests through a Tasty property adapter, preferring `tasty-quickcheck` after verifying its released API and selecting compatible test-only bounds. A property checks a law over generated inputs; shrinking reduces a failing input to a small counterexample, and a replay seed recreates it. Cover monoid identity/associativity and documented right bias for consumer/producer properties and subscriptions, including conflicting keys, empty values, log levels, callback order, and callback poll mode. Observe callback behavior rather than trying to compare functions for equality. Model headers as an ordered list: list round-trip, concatenation, duplicates, empty/binary values, and preservation of order; do not invent nullable header support that the public type cannot represent. Generate record functor/bifunctor/traversal law cases, offset next-position conventions, timestamp/offset boundary representations, text/newtype conversions, supported compression/debug enum mappings, and the actual valid domain of version packing/decoding. Expected values come from a simple independent model or documented native constants, not the implementation being tested. Add fixed edge regressions alongside generated inputs, print replay seeds, and preserve minimized counterexamples. Keep pure tests broker-free so Nix sandbox builds run them.

M2 completes public consumer behavior in both callback modes where applicable: creation with valid/invalid properties, empty/regex subscriptions, assignment replacement and unassignment, pause/resume, position and committed offsets before/after store and commit, synchronous/asynchronous commit reports, disabled auto-store/auto-commit, polling empty/nonempty/error queues, attributed origins, batch boundaries, and fatal-state behavior. Assert exact records/offsets and distinguish expected timeout/EOF from unexpected errors. Batch size zero/negative and timeout sentinels must be checked against documented preconditions in isolated safety probes if needed; do not crash the main test process to test invalid FFI inputs. Check both local helper output and actual broker/consumer effects. Native per-partition errors in APIs besides seek must not be hidden by success-only tests. Multi-consumer assignment/rebalance and outage scenarios belong to EP-15, while this child owns ordinary API state transitions.

For metadata, assert topic/partition identifiers, leader/replica sets consistent with the fixture, low/high watermarks after known production, timestamp-to-offset lookup before/at/after record times, empty/multiple/mixed-invalid requests, missing topics/groups, and metadata timeouts. Test group-state decoding across reachable lifecycle states and unsupported native values through a private conversion fixture; do not merely reproduce the current partial mapping as an expected crash. Keep exact fixtures independent of unrelated topics/groups on the broker. For diagnostic APIs test configuration values, native dump contents via a temporary handle, and documented handle ownership/flush/close behavior; detect descriptor leaks or unexpected closure using owned handles. Check version consistency and compile/run the existing examples and deprecated run wrappers with bounded callbacks. Verify public API claims for threaded/nonthreaded runtimes; if the current code silently fails a claimed mode, record and repair the contract instead of silently removing that matrix row.

M3 reconciles all rows and the measured coverage misses. Strengthen weak inherited assertions instead of only adding cases. Add small deliberate mutations in disposable working copies: wrong offset increment, lost header ordering, reversed property bias, omitted pause, and suppressed error propagation. Focused tests must detect the changed behavior. Do not mutate the working tree's real release candidate or count a compile failure as behavioral proof. Any newly exposed violation of an existing public contract gets a minimized regression and a fix in its owning repair child or here for unassigned functions. Document API changes requiring separate decisions instead of treating them as routine coverage work. Update README/testing documentation to match demonstrated behavior.


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

After selecting and wiring the property adapter, use its locally verified options to record and replay seeds; document the exact invocation in `docs/testing/coverage.md`. Register a stable group `public-contract` for remaining integration cases:

```bash
cabal test rdkafka-hs-test --test-show-details=direct
just test-it '--test-options=-p /public-contract/'
just coverage
python3 scripts/check-coverage.py --allow-open
```

Run `just coverage-check` as a diagnostic before handoff. It may still reject rows owned by incomplete repair/environment children, but no row owned by EP-14 may remain open. Record mutation commands/results from disposable checkouts with only the deliberate semantic mutation applied.


## Validation and Acceptance


All pure laws have fixed boundaries plus generated cases and reproducible failure seeds; no property is tautologically derived from the implementation. Every public export assigned to this child has an observable positive case and each applicable error/boundary case. Broker results assert exact content, counts, offsets, and errors. Hidden/unsupported paths are classified with source-backed rationale rather than skipped. Known defects remain open until fixed. Normal optimized and HPC builds pass, and the selected semantic mutations are caught by runtime assertions. Remaining open rows must be owned explicitly by other unfinished children and still block the master gate. Test-only dependency bounds are registry/tag verified and included correctly in source distributions.


## Idempotence and Recovery


All runs must be repeatable with fresh run IDs. Store generated reports under ignored build/artifact directories; commit the inventory schema, requirements, concise evidence manifest, and commands, not large generated HTML. Do not remove another run's topics or stop a broker not owned by the fixture. On failure retain the seed, operation trace, exact environment, and failure log; restore owned fault fixtures in `finally` before reporting failure. A retry that passes does not erase an earlier failure: diagnose it or keep the row open. Unavailable required environments remain blockers. No tagging, upload, remote publication, or downstream repository mutation is part of this plan.


## Interfaces and Dependencies


Hard dependency on all of `docs/plans/13-make-coverage-evidence-and-test-fixtures-reliable.md`, whose fixtures, inventory schema, and coverage commands must be accepted first. Soft dependencies on `docs/plans/8-apply-topic-creation-configuration-safely.md`, `docs/plans/9-honor-producer-settings-and-release-rejected-callbacks.md`, `docs/plans/10-report-per-partition-seek-failures.md`, `docs/plans/11-own-native-callbacks-through-client-teardown.md`, and `docs/plans/12-release-transaction-error-objects.md`: consume their test evidence but leave ownership of their defects there. This child can complete its own rows while other owners are still pending. EP-15 and the master gate require all repairs and this child Complete.
