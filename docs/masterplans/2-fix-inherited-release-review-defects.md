---
id: 2
slug: fix-inherited-release-review-defects
title: "Fix inherited release-review defects"
kind: master-plan
created_at: 2026-10-24T04:38:40Z
intention: "intention_01m5p37ej8e3h92gkf2wp9j6qa"
provenance:
  created_by:
    model: "gpt-6-astra"
    harness: "codex-cli"
    at: 2026-10-24T04:38:40Z
  revisions:
    - model: "gpt-6-astra"
      harness: "codex-cli"
      at: 2026-10-24T04:58:24Z
      mode: "update"
      note: "Expand acceptance to close public-contract, failure-path, concurrency, ownership, and compatibility coverage gaps; add EP-13 prerequisite and coordinated evidence gates."
    - model: "gpt-6.1-sol"
      harness: "codex-cli"
      at: 2026-10-24T05:04:36Z
      mode: "implement"
      note: "Start EP-13 prerequisite implementation and track validation evidence."
---

# Fix inherited release-review defects


This MasterPlan coordinates eight child ExecPlans. Keep its registry and integration evidence current; children own their implementation milestones. The active Rei intention is `intention_01m5p37ej8e3h92gkf2wp9j6qa`, created beneath project root `intention_01m5gb1h3eehsttdekr9vvvatw` in the personal context `私的`.


## Vision & Scope


After this initiative, topic creation applies supplied settings without crashing, producer settings affect the actual native client, failed seeks report errors, and callbacks and transaction errors have complete allocation-to-release lifetimes. All six defects found in the 2026-10-23 review must have regression evidence and be fixed together in a release-ready working tree. The user expanded this initiative on 2026-10-24 to close coverage gaps across the supported public API, not only the six reported defects. Completion requires a behavior-to-test inventory, trustworthy fixtures, generated-data properties, measured Haskell coverage, deterministic failure and cleanup tests, and an explicit compatibility matrix. A test count or aggregate percentage cannot substitute for missing behavior evidence.

The baseline is commit `9c41b3d`, package version `1.0.0.1`. The review passed 14 unit tests, 116 integration tests against the project-private Redpanda, and `cabal check`; targeted probes nevertheless reproduced five defects. Transaction error leakage was established by inspection of allocation and destruction paths. Those results establish the starting point, not acceptance of future changes. Do not depend on the temporary review script surviving: every child specifies how to reconstruct its regression.

All six defects are inherited. These historical commits are present in this repository's own Git history:

| Finding | Origin in retained history | Owner |
|---|---|---|
| F1: custom topic creation settings throw `Prelude.undefined` | `eb193f5`, May 2025; unreleased upstream code after 5.3.0 | EP-8 |
| F2: producer topic settings are unused | `aa1c2c69`, September 2021; header-producing path stopped applying the topic configuration | EP-9 |
| F3: seek drops per-partition errors | `8d24891`, September 2022 | EP-10 |
| F4: immediate produce failure retains its callback | `f81bec4`, January 2020 | EP-9 |
| F5: native callback function pointers are never freed | Already present in `022a9699`, August 2017 | EP-11 |
| F6: init/begin/abort transaction error objects are never destroyed | `c58bc85`, September 2022 | EP-12 |

F2 through F6 are present in the retained 5.3.0 release commit `e80c322`. Record fixes as inherited defects in the changelog. Preserve public signatures, module names, existing configuration validation conventions, and close/destroy semantics. This initiative does not add admin APIs, a new producer destroy API, unrelated runtime dependency upgrades, new general-purpose broker provisioning platforms, or publication. Test-only dependencies and isolated broker configurations needed to exercise supported behavior are included. There is no fixed cap on test count: enumerate meaningful cases and close them; bounded runtimes keep individual tests diagnosable, not optional. Existing tags are immutable; actual version selection, tag creation, push, and Hackage upload belong to a subsequent release task.


## Decomposition Strategy


Use eight behavior-oriented work streams. EP-13 establishes a trustworthy test harness and coverage inventory before the five repair streams. EP-14 covers the remaining public API and pure-data laws; EP-15 validates combined failure behavior and the declared support matrix. These additional streams are necessary because the original five repair plans alone cannot substantiate library-wide coverage. Keep the original five repair boundaries: Topic administration and consumer seeking each have independent failure semantics. The two producer findings belong together because they share producer setup, enqueue ownership, and the same timeout/queue-full regression fixture. Persistent native callbacks require a separate lifetime design spanning both client types; combining them with the small transaction error fix would obscure that risk. Transaction errors have a short, call-scoped lifetime and can be repaired independently.

Relevant decisions were read before drafting. [ADR-1](../adr/consumer-queue-and-teardown-ownership.md) requires one native-client deallocator, synchronous/idempotent consumer destruction, and callbacks alive through native teardown. [ADR-5](../adr/integration-tests-run-against-a-project-local-redpanda.md) requires run-prefixed resources on port 39092 and prohibits the machine-global broker. [ADR-4](../adr/toolchain-floor-is-ghc-9-12-and-librdkafka-2-15.md) retains GHC 9.12 and librdkafka 2.15 as the floor. [ADR-2](../adr/release-validation-before-publication.md) requires complete-candidate and source-distribution proof before publication. [ADR-7](../adr/first-release-scope-and-distribution.md) documents existing tag distribution and exclusions; it does not authorize publication in this repair initiative.

[ADR-10](../adr/release-coverage-requires-behavior-and-ownership-evidence.md) records the expanded acceptance policy: every supported contract and critical ownership path has evidence, and unresolved gaps cannot be counted as passing. This is an accepted validation policy, not a claim that its tests exist yet. No new cross-repository architecture decision is needed. Dependency source is discovered through `mori://confluentinc/librdkafka`, particularly its `configuration` and `introduction` documentation handles. C ownership contracts are in project-relative `librdkafka/src/rdkafka.h` and implementations in `librdkafka/src/rdkafka.c`, `librdkafka/src/rdkafka_admin.c`, and `librdkafka/src/rdkafka_error.c`; source-file artifact URIs are pending. Resolve the project with Mori; do not traverse `/nix/store` or filesystem root. If implementation proposes different dependency bounds or pins, first verify released versions against the authoritative registry and upstream tags.


## Exec-Plan Registry


| # | Title | Path | Hard Deps | Soft Deps | Status |
|---|---|---|---|---|---|
| 8 | Apply topic creation configuration safely | [EP-8](../plans/8-apply-topic-creation-configuration-safely.md) | EP-13 | None | Complete |
| 9 | Honor producer settings and release rejected callbacks | [EP-9](../plans/9-honor-producer-settings-and-release-rejected-callbacks.md) | EP-13 | None | Complete |
| 10 | Report per-partition seek failures | [EP-10](../plans/10-report-per-partition-seek-failures.md) | EP-13 | None | Complete |
| 11 | Own native callbacks through client teardown | [EP-11](../plans/11-own-native-callbacks-through-client-teardown.md) | EP-13 | EP-9 | In Progress |
| 12 | Release transaction error objects | [EP-12](../plans/12-release-transaction-error-objects.md) | EP-13 | None | Not Started |
| 13 | Make coverage evidence and test fixtures reliable | [EP-13](../plans/13-make-coverage-evidence-and-test-fixtures-reliable.md) | None | None | Complete |
| 14 | Cover the public API and pure-data laws | [EP-14](../plans/14-cover-the-public-api-and-pure-data-laws.md) | EP-13 | EP-8, EP-9, EP-10, EP-11, EP-12 | In Progress |
| 15 | Validate broker failures and supported environments | [EP-15](../plans/15-validate-broker-failures-and-supported-environments.md) | EP-8, EP-9, EP-10, EP-11, EP-12, EP-13, EP-14 | None | Not Started |


## Dependency Graph


Default execution order is EP-13, EP-8, EP-9, EP-10, EP-11, EP-12, EP-14, EP-15, then the combined acceptance gate below. EP-13 must be Complete before any dependent child begins: its fixtures must fail on injected errors, its inventory must assign every contract, and its coverage command must emit valid candidate evidence. Each repair and EP-14 has this whole-child hard dependency. EP-15 requires all seven earlier children Complete so compatibility and fault tests exercise the repaired and locally verified candidate. No repair depends on another repair as a hard prerequisite. EP-11 benefits from EP-9's producer regressions when changing construction and destruction, hence the soft dependency. EP-9 and EP-11 have an integration dependency: the delivery callback registration must outlive every accepted message callback, while an enqueue failure must not transfer ownership. EP-11 must run that representative producer path before expanding its consumer and failure-path matrix.

After EP-13, the five repairs and EP-14 can proceed independently if shared-file edits are coordinated. EP-14 owns only contracts not already assigned to a repair; pending evidence owned by another child remains visible in the inventory and prevents the final gate from passing. The combined gate has a hard dependency on every child being Complete, including its evidence. It is owned by the implementing coordinator and is not implicitly satisfied by individual green tests.


## Integration Points


EP-13 owns `test-integration/Kafka/TestEnv.hs`, reusable fault/GC helpers, test-runner mechanics, and `scripts/check-coverage.py`, `scripts/test-coverage.sh`, `docs/testing/coverage-matrix.json`, and `docs/testing/coverage.md`. The JSON inventory records each public operation or behavioral group, applicable success/error/boundary/lifetime cases, owner, exact test names, evidence revision, and status. All children update their rows; EP-13 owns schema and validation. New exports or unassigned rows fail the inventory check. Do not hide missing tests behind `isRight`, nonempty-list checks, skipped tests, `rights`, or ignored error returns.

A child is Complete when its own contract rows and milestones pass; cross-environment or security cases explicitly owned by EP-15 remain separate open rows until that child runs. Cross-links must not introduce a reverse dependency from a repair to EP-15. The master gate still requires every row closed.

EP-14 owns generated-data property suites and the remaining consumer/metadata/dump/version/type contracts. EP-15 owns `scripts/test-compatibility.sh`, run-owned broker failure fixtures, actual authentication/authorization checks, and `docs/testing/compatibility-matrix.json`. EP-15 may extend test-only configuration in `flake.module.nix`, `justfile`, or dedicated scripts; do not edit seihou-managed scaffolding to add a second toolchain. The matrix must distinguish tested environments from untested possibilities allowed by broad dependency bounds, and include a primary supported tuple, a Linux tuple, an Apache Kafka tuple, and a newer compatible compiler/library tuple. Select concrete released versions during implementation using authoritative registries/tags. Existing runtime pins stay intact. Missing access to a required environment is a blocker, not a successful skip.

EP-11 owns the callback-registration owner and native-client lifetime changes in `src/Kafka/Internal/Setup.hs`, `src/Kafka/Internal/RdKafka.chs`, and the client constructors. EP-9 owns effective producer configuration and per-message `StablePtr` transfer in `src/Kafka/Producer.hs` and `src/Kafka/Producer/Callbacks.hs`. EP-11 must adapt its constructor changes to EP-9 without replacing global topic settings or freeing accepted message callbacks prematurely. The pointer used to let C call Haskell is a `FunPtr`; the per-message root retaining a Haskell closure is a separate `StablePtr`. They must not share one undifferentiated cleanup list.

EP-10 owns a narrow internal partition-error reader in `src/Kafka/Consumer/Convert.hs`; it must not change all existing partition-list conversions or expose new public result types. EP-12 owns transaction error bracketing and must not broaden public transaction return types. EP-8 owns topic-admin handle cleanup, including configuration failure before request submission. All changes to the binding module require regeneration and verification of `docs/bindings.md`.

Each child registers its tests in `test-integration/Main.hs` and `rdkafka-hs.cabal` as needed, using distinct test modules and additive edits. EP-13 introduces and owns the shared weak-reference helper in `test-integration/Kafka/CallbackTestSupport.hs`; EP-9 and EP-11 extend it through that owner as their cases require. A weak reference observes whether captured state can be collected without keeping it alive itself. Tests must prove both retention while a callback is needed and collection after release, with bounded garbage-collection retries and helper functions that prevent accidental stack retention.

EP-8 confirms administration remains usable after graceful consumer close in both callback modes. Generic `HasKafka` operations retain only the native client ForeignPtr, so EP-11 must couple callback registration ownership to that pointer's native teardown rather than relying exclusively on the surrounding client record staying reachable.

An isolated debug-logging diagnostic also crashes when the native topic C finalizer calls back into Haskell. EP-11 must cover native topic finalization as well as client teardown: callback-capable destruction cannot run through a C finalizer. The original failure log is `/tmp/rdkafka-seek-debug.log`.

EP-11 must update ADR-1 with actual ownership and finalizer ordering when implemented. EP-9 must document precedence between global and explicit topic properties. Promote that precedence to an ADR only if it becomes a distinct lasting project contract; do not describe an unimplemented ownership design as already accepted. Other children distill any new durable constraints under the existing profiled ADR workflow.

The coordinator owns final reconciliation: all six finding IDs and every supported public contract map to accepted tests; changelog and Haddocks describe the repaired behavior; source-distribution builds include all needed sources; public API compatibility and shutdown behavior remain intact. From the repository root, run:

```bash
nix develop -c cabal build all -f examples -f integration
nix develop -c cabal test rdkafka-hs-test --test-show-details=direct
nix develop -c just test-it
nix develop -c cabal check
nix develop -c cabal sdist
nix develop -c just check
nix develop -c just coverage
nix develop -c just coverage-check
nix develop -c just test-compatibility
```

Build and unit-test the emitted tarball from a fresh temporary extraction under the same development environment; follow the concrete candidate validation recipe in `docs/plans/6-cut-the-first-rdkafka-hs-release-and-migration-guide.md` without its publication steps. Record the source revision, linked librdkafka version, actual test counts, tarball-build result, coverage artifact locations, compatibility tuples, and absence of unexplained open rows here. `just coverage` and `just coverage-check` are delivered by EP-13; `just test-compatibility` is delivered by EP-15. Instrumented and normal optimized builds must both pass: Haskell coverage cannot prove C allocation balance or eliminate optimizer/concurrency differences. All hand-written source coverage misses must be classified, with critical error/ownership branches actually exercised. Only source-proven unreachable or inapplicable cases can be marked not applicable; convenience, environment unavailability, and a large passing count are not exemptions. No known defect may be accepted as an expected failure to close a row. Add regression checks for newly discovered violations of existing contracts and assign their repair to the owning child; API redesigns remain separate scope decisions. A candidate source change invalidates affected evidence. Complete the ADR distillation pass and leave a handoff suitable for the separate release workflow; do not move tags, upload, or mark the repair intention complete merely because these planning documents exist.


## Progress


EP-13 is Complete. Its accepted baseline passes 14 unit and 129 integration tests in normal and instrumented builds, plus 15 tooling controls. The compiler-derived inventory contains 174 distinct exports and 169 contract/resource/environment rows; 168 remain open. All 22 library modules appear in measured reports, including explicit zero coverage for unlinked functions. The final coverage gate correctly rejects open rows and 3,004 unexplained hand-written misses. See [EP-13](../plans/13-make-coverage-evidence-and-test-fixtures-reliable.md) and its [evidence manifest](../testing/coverage-evidence.json).

EP-8 and EP-9 are Complete. Admin evidence covers 53 focused cases and eight compiled semantic mutations; its unchanged owned source scope remains verified after independent producer changes. Producer evidence covers 61 focused cases, thirteen compiled mutations, effective configuration, error-object ownership/code propagation, byte boundaries, exceptions, observed races and queued/in-flight purge flags. EP-14's shared adapter/producer-composition milestone adds generated and replayable laws with four compiled/shrunk/replayed bias/order mutations. The rest of EP-14 is In Progress.

EP-10 is Complete: 76 focused seek/rewind cases and seven compiled mutations verify ordered member failures, partial success, initiation-only zero timeout, offset/log boundaries and assignment changes during rewind. Current normal and instrumented suites pass 21 unit and 320 integration tests (156.82/149.39 seconds), with child probe coverage merged; 18 tooling controls and strict ADR/Cabal/binding/Nix checks pass. The inventory has 181 rows: 17 tested and 164 explicitly open. See the [current coverage manifest](../testing/coverage-evidence.json). Resume EP-11 and EP-12, then the remaining EP-14 work; EP-15 still waits for all earlier children. The saved intermittent deprecated-seek failure remains an open EP-14 obligation, and the native C-finalizer callback crash belongs to EP-11. Green reruns do not close either discovery.

- [ ] Combined candidate closes all supported contract and critical ownership rows, six original regressions, generated-data properties, normal/coverage builds, source-distribution validation, broker-failure tests, and the required compatibility matrix.


## Surprises & Discoveries

EP-11's teardown audit identified a possible unowned remainder when batch message conversion is interrupted. EP-14 owns reproduction and full-batch cleanup under its existing polling/batch contract; its plan records the concrete source path. This remains an open obligation, independent of callback registration release.


The coverage audit found that `test-integration/Kafka/TestEnv.hs:sendMessages` and `sendMessagesWithHeaders` ignore enqueue results, and `receiveMessages` filters errors with `rights`. Existing metadata checks often assert only `isRight` or collection length. EP-13 must repair these false-success paths before new test results can be trusted. The interrupted-destroy test uses `forkIO` followed by `killThread` without proving destruction reached a particular stage; EP-11 must add controlled barriers around meaningful teardown windows.

EP-13 found that c2hs `LINE` pragmas suppress GHC coverage for the hand-written binding wrappers, while unlinked modules disappear from runtime TIX data. Coverage-only source mapping and matching-hash zero indices now retain both categories. These are validation mechanics; native allocation balance remains a separate obligation.

The existing suites pass despite all six defects, so their results alone cannot establish release readiness. The review's queue-full probe retained callback state after a major garbage collection; a separate probe retained an error callback's state after explicit consumer destruction. These are different ownership defects and need different negative controls.

Producer configuration requires merging rather than blindly installing an empty topic configuration: librdkafka's default-topic replacement can overwrite topic-level settings already supplied globally. Preserve existing `extraProp` behavior when restoring `sendTimeout` and `topicCompression`.


## Decision Log


2026-10-23: Group producer settings and per-message callback ownership in EP-9; isolate persistent native callback ownership in EP-11. This keeps setup/enqueue regression fixtures together while making client teardown risk explicit.

2026-10-23: Preserve public API shapes and existing lifecycle semantics; fix behavior and ownership internally. Publication is a separate task because the user requested repair planning and existing tags already exist.

2026-10-23: Link this MasterPlan and all five children to the newly created personal Rei intention under the existing project root. Do not reuse the earlier fork initiative's intention or create five additional intentions without a request.


2026-10-24: Integrate EP-14's shared property adapter and generated producer-composition cases before closing EP-9 M3. Keep the remaining EP-14 milestones open and then resume the declared repair sequence. Align inventory ownership with the plans: EP-10 owns replacement seek and committed-offset rewind; EP-14 owns deprecated seek and its saved intermittent failure.

2026-10-24 revision: The user requested good coverage without gaps. Add EP-13 through EP-15, make trustworthy fixtures a prerequisite, strengthen each repair's acceptance matrix, and require public-contract/ownership evidence plus supported-environment validation. Preserve the original intention and repair scope; do not claim infinite input or schedule coverage.


## Outcomes & Retrospective


Not implemented. Fill in accepted outcomes and remaining limitations after the combined candidate gate, including durable ADR updates.
