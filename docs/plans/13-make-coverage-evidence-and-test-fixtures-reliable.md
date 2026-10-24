---
id: 13
slug: make-coverage-evidence-and-test-fixtures-reliable
title: "Make coverage evidence and test fixtures reliable"
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

# Make coverage evidence and test fixtures reliable


This ExecPlan is a living document. Keep its evidence, milestones, dependencies, and decisions current.


## Purpose / Big Picture


Make test success trustworthy and make missing coverage visible. A maintainer can run one command to collect Haskell coverage and another to reject an incomplete coverage inventory. Every supported public operation and critical native-resource path has a named owner and a precise behavioral assertion, even before later plans implement those assertions.


## Progress


- [ ] M1: Fixtures propagate unexpected errors, assert exact results, and recover run-owned resources on partial setup/failure.
- [ ] M2: A machine-checked public-contract and native-ownership inventory identifies all known gaps and assigns them to children.
- [ ] M3: Reproducible normal/instrumented coverage runs produce source-attributed reports, and the final coverage gate rejects missing or stale evidence.

Not started. The checks described below are deliverables to implement, not existing evidence.


## Surprises & Discoveries


`sendMessages` and `sendMessagesWithHeaders` currently ignore every `produceMessage` result and return `Right ()`. `receiveMessages` uses `rights` and treats any error as an end condition, so it cannot distinguish expected timeout from real failure. Several inherited tests assert only `isRight` or nonempty results. These false-success paths explain why the existing count cannot substantiate comprehensive coverage.


## Decision Log


2026-10-24: Establish fixtures and evidence before dependent repairs. The baseline inventory may contain open rows with explicit owners; the final gate must fail on those rows. No arbitrary test-count or aggregate-percentage threshold can excuse an uncovered supported behavior.


## Outcomes & Retrospective


Not implemented. Record observed results and durable lessons at completion; update relevant ADRs and the parent registry.


## Context and Orientation


This child owns `test-integration/Kafka/TestEnv.hs`, test runner mechanics, reusable `test-integration/Kafka/CallbackTestSupport.hs` and `FaultTestSupport.hs`, `scripts/test-coverage.sh`, `scripts/check-coverage.py`, and new files under `docs/testing/`. Existing broker tests are deliberately ordered and share a topic/group. Make new fixtures isolated and repair misleading helpers without pretending every existing dependent sequence is independent. Keep native callback/resource implementation ownership with EP-8 through EP-12.

This repository is `rdkafka-hs`, a Haskell wrapper around librdkafka. The reviewed baseline is `9c41b3d`; planning commit `7646e69` added the six-defect repair initiative. `rdkafka-hs.cabal` lists the public modules and two test suites: `rdkafka-hs-test` has 14 baseline tests and `rdkafka-hs-integration` has 116. `test/Main.hs` and `test-integration/Main.hs` register cases; `test-integration/Kafka/TestEnv.hs` owns broker fixtures. Generated bindings come from `src/Kafka/Internal/RdKafka.chs`, with hand-written conversion and lifetime code alongside them.

Read [ADR-10](../adr/release-coverage-requires-behavior-and-ownership-evidence.md): coverage means evidence for each supported behavior and critical ownership branch, not a passing count. [ADR-5](../adr/integration-tests-run-against-a-project-local-redpanda.md) protects the shared broker: use the private endpoint at `127.0.0.1:39092`, run-prefixed resources, and only run-owned fault targets. [ADR-1](../adr/consumer-queue-and-teardown-ownership.md) distinguishes graceful close from destruction and requires one native deallocator. [ADR-4](../adr/toolchain-floor-is-ghc-9-12-and-librdkafka-2-15.md) retains the existing compiler/library floor; [ADR-2](../adr/release-validation-before-publication.md) requires exact-candidate validation before publication. Existing first-release platform/provisioning limits in [ADR-7](../adr/first-release-scope-and-distribution.md) describe the earlier release; the new compatibility plan must document its additional test environments without rewriting that release history.

Use Mori to locate dependency sources before relying on APIs. `mori://confluentinc/librdkafka` owns the native API. `mori://haskell/cabal` owns project-relative `cabal/doc/cabal-project-description-file.rst`, whose coverage section documents `--enable-coverage`. `mori://UnkindPartition/tasty/packages/tasty-quickcheck` supplies a property-test adapter; its source is under project-relative `tasty/quickcheck/Test/Tasty/QuickCheck.hs` in `mori://UnkindPartition/tasty`. Source-file artifact URIs are pending. Consult the local source first; verify current releases through authoritative package registries/upstream tags before choosing new test dependency bounds. No runtime dependency upgrade is implied. Do not inspect `/nix/store` or filesystem root.


## Plan of Work


M1 replaces success fabrication with explicit expected-error policies. Enqueue helpers fail on immediate error and wait for/assert the intended delivery reports. Receive helpers return exact requested records or a diagnostic error, permitting only explicitly expected timeout/EOF outcomes; never discard arbitrary `Left` values. Timeout budgets use a monotonic deadline. Partial fixture construction must release already acquired clients/topics if a later step fails. Consumer fixtures destroy after graceful close where appropriate; unreachable-producer cleanup purges before any potentially unbounded flush. Add harness self-tests that inject enqueue failure, unexpected poll error, missing/extra records, delivery failure, and setup failure; prove each produces a failing assertion/result rather than green output. Use fake operations only to test the helper itself; retain actual broker round-trips for library contracts. Add reusable barriers for thread-stage synchronization and weak-reference controls without public production hooks.

M2 creates `docs/testing/coverage-matrix.json` and the human guide `docs/testing/coverage.md`. Inventory `exposed-modules`, exported functions/types and supported re-exports from `rdkafka-hs.cabal` and `src/Kafka`, including deprecated wrappers, diagnostics, pure helpers, and properties. Define rows with stable ID, public symbols, behavior/inputs, expected result, mode applicability, ownership transitions, child owner, exact registered test names, status, evidence revision/configuration, and rationale for any not-applicable case. Group trivial accessors only when one test genuinely proves their shared contract. Distinguish “unimplemented”, “tested”, and “not applicable”; tests that fail are not “tested passing”. Inventory critical internal allocation/transfer/release and error-decoding branches separately. Map F1–F6 to their repair children and all remaining contracts to EP-14 or EP-15. Build `scripts/check-coverage.py` to verify row completeness, symbol references, test-name existence, source revision/evidence compatibility, and unclassified coverage misses. Validate public export inventory against compiler-derived exports or a deliberately scoped parser checked against Haddocks; a regex that silently drops multiline exports/re-exports is insufficient. Expose `--allow-open` for development inventory/schema validation; production `just coverage-check` must never use it.

M3 implements `scripts/test-coverage.sh` and `just coverage`. Use Cabal `--enable-coverage` for both test suites in a separate build directory and generate per-suite and combined reports from matching build instrumentation. Record expression, alternative, and boolean-condition metrics that HPC actually supplies; do not call those metrics C coverage or infer memory safety from them. Keep generated c2hs boilerplate separate from hand-written wrappers; never exclude all of `RdKafka.chs` to hide hand-written misses. Missing source maps or unmerged suite data fail report generation instead of yielding an inflated percentage. Reports must identify every hand-written function not exercised, with a matrix owner or source-backed reason. Save the candidate revision, dirty-tree digest when applicable, toolchain/native/broker versions, exact commands, test names, and failure seeds. Fingerprint measured source, test, and build/configuration inputs separately from generated reports and evidence-only documentation so recording a result cannot invalidate that same result. A temporary untested export must make the inventory checker fail; stale test names, missing TIX data, and stale source evidence must also fail. Normal optimized tests remain a separate required run. Document how to refresh evidence after implementation changes.


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

Baseline instrumentation, inside the development shell after `just redpanda-up`:

```bash
cabal test rdkafka-hs-test rdkafka-hs-integration -f integration --enable-coverage --builddir=dist-coverage --test-show-details=direct
```

The delivered commands are:

```bash
just coverage
python3 scripts/check-coverage.py --allow-open
just coverage-check
```

At this child's completion the first two must succeed; the last must reject known open repair/API/environment rows with useful IDs and owners. That is an intentional negative test of the gate, not a waiver for the eventual release. Once all children finish, the same final gate must pass with no development option.


## Validation and Acceptance


A deliberately failed enqueue/poll/setup cannot yield a passing helper result. Exact identity/count/offset assertions replace success-only checks wherever these helpers are used. Every public export and critical internal ownership path is mapped; no orphan owner or test name is accepted. Reports include both test suites from one candidate and explicitly separate generated code. Removing a test, changing an export, or modifying source without new evidence makes the appropriate gate fail. Not-applicable entries include an exact precondition or unreachable-path explanation and cannot be used for inconvenient failure injection or unavailable environments. EP-13 is Complete only when the baseline inventory is exhaustive, fixture self-tests pass, instrumentation works, and the rejecting final gate is demonstrated; passing the final gate is deferred to all-child completion.


## Idempotence and Recovery


All runs must be repeatable with fresh run IDs. Store generated reports under ignored build/artifact directories; commit the inventory schema, requirements, concise evidence manifest, and commands, not large generated HTML. Do not remove another run's topics or stop a broker not owned by the fixture. On failure retain the seed, operation trace, exact environment, and failure log; restore owned fault fixtures in `finally` before reporting failure. A retry that passes does not erase an earlier failure: diagnose it or keep the row open. Unavailable required environments remain blockers. No tagging, upload, remote publication, or downstream repository mutation is part of this plan.


## Interfaces and Dependencies


No prerequisites; this is the first child to implement. All of EP-8 through EP-12 and EP-14 depend on this entire child. `docs/masterplans/2-fix-inherited-release-review-defects.md` owns final no-open-row acceptance. EP-14 supplies public/pure test cases; EP-15 supplies environment and fault cases. They may extend this child's schema only through a coordinated update that preserves existing evidence. Test-only modules remain private and source-distribution-complete.
