---
id: 8
slug: apply-topic-creation-configuration-safely
title: "Apply topic creation configuration safely"
kind: exec-plan
created_at: 2026-10-24T04:38:50Z
intention: "intention_01m5p37ej8e3h92gkf2wp9j6qa"
master_plan: "docs/masterplans/2-fix-inherited-release-review-defects.md"
provenance:
  created_by:
    model: "gpt-6-astra"
    harness: "codex-cli"
    at: 2026-10-24T04:38:50Z
  revisions:
    - model: "gpt-6-astra"
      harness: "codex-cli"
      at: 2026-10-24T04:58:24Z
      mode: "update"
      note: "Expand acceptance to close public-contract, failure-path, concurrency, ownership, and compatibility coverage gaps; add EP-13 prerequisite and coordinated evidence gates."
---

# Apply topic creation configuration safely


This ExecPlan is a living document. Keep Progress, Surprises & Discoveries, Decision Log, and Outcomes & Retrospective current.


## Purpose / Big Picture


Fix F1: applications can create a topic with settings such as `cleanup.policy=compact` without a Haskell exception. The broker receives those settings, and failures release the native request object and return through the existing `Either KafkaError TopicName` result.


## Progress


- [ ] M1: Nonempty valid topic settings are submitted successfully and verified on the private broker.
- [ ] M2: Empty settings, rejected settings, and interrupted/error construction paths preserve results and release owned handles.

- [ ] M3: Topic administration covers meaningful input boundaries, broker errors, and request/event lifetime paths with verified broker state.

Not started; this document records the implementation contract, not completed fixes.


## Surprises & Discoveries


The review reproduced `Prelude.undefined` with a one-entry configuration map. The bug was introduced by retained commit `eb193f5` in May 2025, after the upstream 5.3.0 release; it was imported into this fork. `TopicTest.hs` only creates topics with `mempty`, which avoids the failing expression.


## Decision Log


2026-10-24 revision: The user expanded acceptance to close coverage gaps. Add the complete behavioral/ownership matrix for this workstream, depend on EP-13's reliable fixtures, and require evidence per coverage-inventory row. Existing milestones remain required.

2026-10-23: Repair the existing create-topic path without adding a public admin configuration API. Prove the supplied value reaches the broker through a test-only broker inspection or a supported behavioral assertion.


## Outcomes & Retrospective


Not implemented. Record accepted evidence, remaining limitations, and durable lessons here at completion; update relevant ADRs before marking Complete.


## Context and Orientation


`src/Kafka/Topic.hs` calls `mkNewTopicUnsafe` through `withNewTopic`/`withUnsafeOne`. The constructor allocates `topic'` and then calls `rdKafkaNewTopicSetConfig undefined`. The outer bracket sees no allocated handle when configuration fails inside its acquisition, so merely replacing `undefined` does not complete error cleanup. The native request object is allocated by `newRdKafkaNewTopicUnsafe` and destroyed by `rdKafkaNewTopicDestroy` in `src/Kafka/Internal/RdKafka.chs`. Extend `test-integration/Kafka/Integration/TopicTest.hs` or create a dedicated `TopicConfigurationTest.hs`.

[ADR-7](../adr/first-release-scope-and-distribution.md) excludes expanding the admin API. Retain existing create/delete signatures and timeout behavior unless a change is essential to prove this repair; document such a discovery rather than hiding it in a configuration fix.

This is the Haskell package `rdkafka-hs`, reviewed at commit `9c41b3d` (version `1.0.0.1`). The C library librdkafka performs the native work; `src/Kafka/Internal/RdKafka.chs` generates Haskell bindings through c2hs. Use the project Nix development shell: plain Cabal in the review environment could not find the C library. [ADR-4](../adr/toolchain-floor-is-ghc-9-12-and-librdkafka-2-15.md) retains GHC 9.12 and librdkafka 2.15. No dependency upgrade is required.

[ADR-5](../adr/integration-tests-run-against-a-project-local-redpanda.md) requires the private broker at `127.0.0.1:39092`, random run-prefixed topics/groups, and cleanup limited to that run. Existing fixtures are in `test-integration/Kafka/TestEnv.hs`; register new modules in `test-integration/Main.hs` and the integration suite's `other-modules` in `rdkafka-hs.cabal`. Never use the shared port 9092. The baseline passed 14 unit and 116 integration tests, so add a regression that distinguishes this defect from the baseline.

Resolve dependency sources with Mori before changing bindings. The source project is `mori://confluentinc/librdkafka`; its project-relative `librdkafka/src/rdkafka.h` describes ownership, with implementation in the adjacent C files (source-file artifact URIs pending). Discover its location with the commands below and inspect only the scoped checkout, never `/nix/store` or filesystem root. Keep existing pins; any proposed bound change requires checking the authoritative registry and upstream release tags.


## Plan of Work


M1 replaces the undefined pointer with the actual allocated topic handle and adds a failing-before/passing-after public `createTopic` regression with a nonempty map. Use a fresh topic with a supported configuration, preferably `cleanup.policy=compact`, and verify the broker's effective setting with the local Redpanda inspection mechanism. Discover the actual `rpk`/container invocation from `nix/redpanda.nix` and the available CLI help; record the exact command and expected `compact` value in this plan. A returned `Right` alone is insufficient to prove configuration is not dropped.

M2 moves allocation, configuration, submission, and cleanup under one explicit ownership boundary. Keep the handle alive through request submission, then destroy it once on success, rejected configuration, or exceptions. Either acquire the raw handle under the outer bracket and configure in its body, or use equivalent scoped ownership that cannot lose a handle when acquisition returns `Left`. Do not install a finalizer and retain an unconditional manual destroy on the same handle. Inspect the native setter before designing a local rejection fixture: some invalid settings are rejected by the broker, not by the setter. Cover a broker-rejected value and, with a narrow internal test seam if needed, an exception after allocation but before submission. Add the inherited-fix changelog entry.


M3 expands topic administration coverage beyond the original crash. Use empty, singleton, and multiple configuration maps; supported values must be verified on the broker. Exercise invalid names, invalid partition/replication parameters, unsupported configuration, duplicate creation, deletion of an existing topic, and deletion of a nonexistent topic. Assert the documented result and actual broker state, not merely `isRight`. Exercise unavailable/slow broker responses and the native event-queue timeout/error branches with bounded, controlled fixtures. Inspect `waitForResponse` so a missing/wrong event cannot masquerade as a successful empty result. Test request allocation, configuration, submission, response conversion, and cleanup failures using private seams only where the real broker cannot force the window reliably. Each owned request/event/queue/options resource touched by these paths has a documented release boundary and a negative control. Correct defects exposed in the existing create/delete contract; do not broaden the public admin surface.

## Concrete Steps


Run from `/Users/shinzui/Keikaku/bokuno/rdkafka-hs` (or the root of the current checkout). Enter the development shell, then run commands in it:

```bash
nix develop
mori registry search librdkafka
mori registry show confluentinc/librdkafka --full
mori registry docs confluentinc/librdkafka
cabal build all -f integration
```

After the focused regression, run:

```bash
cabal test rdkafka-hs-test --test-show-details=direct
just test-it
cabal check
```

If `src/Kafka/Internal/RdKafka.chs` changes, also run `just bindings-inventory` and `just bindings-check` in that shell; the inventory must match. Preserve unrelated work, including any pre-existing `.seihou/manifest.json` edit. Update the child's evidence and the parent registry after acceptance; use Conventional Commits with this child's `ExecPlan:`, the parent `MasterPlan:`, and `Intention: intention_01m5p37ej8e3h92gkf2wp9j6qa` trailers when implementation is committed.

Use the focused group (give any new group this exact name):

```bash
just test-it '--test-options=-p /topic-configuration/'
```

Record the broker-inspection command chosen in M1 and native handle-release evidence in this document. The focused group must fail on the original undefined expression and pass after the repair.


## Validation and Acceptance


A valid nonempty configuration returns `Right` and the broker exposes the requested setting. Empty configuration still creates a usable topic. Invalid broker configuration returns `Left` instead of a Haskell exception and does not leave a created topic mistaken for success. Construction/submission failure and exceptions release the request object exactly once; prove with a narrow allocation/destruction counter around the scoped helper or a native leak diagnostic, and include a positive control showing the old acquisition pattern retains the object. Existing topic create/delete tests remain green. Cleanup operates only on the test's unique topic.


The topic-admin rows in `docs/testing/coverage-matrix.json` must name tests for each supported success/failure branch above and their release evidence. The positive configuration test must fail if configuration application is deliberately removed, not just if `undefined` is restored. Repeated failures must not leak request objects, queues, or response events. A timeout must be distinguishable from successful creation; a failed create cannot be accepted by a test that ignores the broker's actual state. Broker ACL denial is exercised by EP-15 and cross-linked here; this child owns local input and ordinary admin failure semantics.

All new cases use EP-13's failure-preserving fixtures and are recorded with exact test names and candidate evidence in the coverage inventory. No known failing case may be marked expected-failure to claim completion.

## Idempotence and Recovery


These changes require no data migration. Re-running tests uses fresh run prefixes and must delete only resources created by that run. Use bounded waits and `bracket`/`finally` for fixture cleanup; a failed assertion must not leave a producer flushing indefinitely. Do not reset the shared broker or overwrite unrelated edits. If a proof fails, leave the milestone unfinished, record the failing behavior, and fix the implementation or fixture before broadening the test matrix. Do not publish or move release tags.


## Interfaces and Dependencies


Hard dependency: `docs/plans/13-make-coverage-evidence-and-test-fixtures-reliable.md` must be Complete; its trustworthy fixtures and coverage inventory are prerequisites. No other hard dependency. `docs/masterplans/2-fix-inherited-release-review-defects.md` owns the combined release gate. This child owns `src/Kafka/Topic.hs`, its topic-request bindings if necessary, and topic configuration tests. EP-11 in `docs/plans/11-own-native-callbacks-through-client-teardown.md` owns client callback lifetimes; native topic request objects must remain a separate scoped resource. Public `NewTopic` and `createTopic` types remain unchanged.
