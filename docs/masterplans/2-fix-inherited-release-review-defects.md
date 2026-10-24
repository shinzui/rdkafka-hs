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
---

# Fix inherited release-review defects


This MasterPlan coordinates five child ExecPlans. Keep its registry and integration evidence current; children own their implementation milestones. The active Rei intention is `intention_01m5p37ej8e3h92gkf2wp9j6qa`, created beneath project root `intention_01m5gb1h3eehsttdekr9vvvatw` in the personal context `私的`.


## Vision & Scope


After this initiative, topic creation applies supplied settings without crashing, producer settings affect the actual native client, failed seeks report errors, and callbacks and transaction errors have complete allocation-to-release lifetimes. All six defects found in the 2026-10-23 review must have regression evidence and be fixed together in a release-ready working tree.

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

F2 through F6 are present in the retained 5.3.0 release commit `e80c322`. Record fixes as inherited defects in the changelog. Preserve public signatures, module names, existing configuration validation conventions, and close/destroy semantics. This initiative does not add admin APIs, a new producer destroy API, dependency upgrades, broker provisioning platforms, or publication. Existing tags are immutable; actual version selection, tag creation, push, and Hackage upload belong to a subsequent release task.


## Decomposition Strategy


Use five behavior-oriented work streams. Topic administration and consumer seeking each have independent failure semantics. The two producer findings belong together because they share producer setup, enqueue ownership, and the same timeout/queue-full regression fixture. Persistent native callbacks require a separate lifetime design spanning both client types; combining them with the small transaction error fix would obscure that risk. Transaction errors have a short, call-scoped lifetime and can be repaired independently.

Relevant decisions were read before drafting. [ADR-1](../adr/consumer-queue-and-teardown-ownership.md) requires one native-client deallocator, synchronous/idempotent consumer destruction, and callbacks alive through native teardown. [ADR-5](../adr/integration-tests-run-against-a-project-local-redpanda.md) requires run-prefixed resources on port 39092 and prohibits the machine-global broker. [ADR-4](../adr/toolchain-floor-is-ghc-9-12-and-librdkafka-2-15.md) retains GHC 9.12 and librdkafka 2.15 as the floor. [ADR-2](../adr/release-validation-before-publication.md) requires complete-candidate and source-distribution proof before publication. [ADR-7](../adr/first-release-scope-and-distribution.md) documents existing tag distribution and exclusions; it does not authorize publication in this repair initiative.

No new cross-repository architecture decision is needed. Dependency source is discovered through `mori://confluentinc/librdkafka`, particularly its `configuration` and `introduction` documentation handles. C ownership contracts are in project-relative `librdkafka/src/rdkafka.h` and implementations in `librdkafka/src/rdkafka.c`, `librdkafka/src/rdkafka_admin.c`, and `librdkafka/src/rdkafka_error.c`; source-file artifact URIs are pending. Resolve the project with Mori; do not traverse `/nix/store` or filesystem root. If implementation proposes different dependency bounds or pins, first verify released versions against the authoritative registry and upstream tags.


## Exec-Plan Registry


| # | Title | Path | Hard Deps | Soft Deps | Status |
|---|---|---|---|---|---|
| 8 | Apply topic creation configuration safely | [EP-8](../plans/8-apply-topic-creation-configuration-safely.md) | None | None | Not Started |
| 9 | Honor producer settings and release rejected callbacks | [EP-9](../plans/9-honor-producer-settings-and-release-rejected-callbacks.md) | None | None | Not Started |
| 10 | Report per-partition seek failures | [EP-10](../plans/10-report-per-partition-seek-failures.md) | None | None | Not Started |
| 11 | Own native callbacks through client teardown | [EP-11](../plans/11-own-native-callbacks-through-client-teardown.md) | None | EP-9 | Not Started |
| 12 | Release transaction error objects | [EP-12](../plans/12-release-transaction-error-objects.md) | None | None | Not Started |


## Dependency Graph


Default execution order is EP-8, EP-9, EP-10, EP-11, EP-12, then the combined acceptance gate below. No child has a hard dependency on another repair. EP-11 benefits from EP-9's producer regressions when changing construction and destruction, hence the soft dependency. EP-9 and EP-11 have an integration dependency: the delivery callback registration must outlive every accepted message callback, while an enqueue failure must not transfer ownership. EP-11 must run that representative producer path before expanding its consumer and failure-path matrix.

Children can be implemented independently if shared-file edits are coordinated. The combined gate has a hard dependency on every child being Complete, including its evidence. It is owned by the implementing coordinator and is not implicitly satisfied by individual green tests.


## Integration Points


EP-11 owns the callback-registration owner and native-client lifetime changes in `src/Kafka/Internal/Setup.hs`, `src/Kafka/Internal/RdKafka.chs`, and the client constructors. EP-9 owns effective producer configuration and per-message `StablePtr` transfer in `src/Kafka/Producer.hs` and `src/Kafka/Producer/Callbacks.hs`. EP-11 must adapt its constructor changes to EP-9 without replacing global topic settings or freeing accepted message callbacks prematurely. The pointer used to let C call Haskell is a `FunPtr`; the per-message root retaining a Haskell closure is a separate `StablePtr`. They must not share one undifferentiated cleanup list.

EP-10 owns a narrow internal partition-error reader in `src/Kafka/Consumer/Convert.hs`; it must not change all existing partition-list conversions or expose new public result types. EP-12 owns transaction error bracketing and must not broaden public transaction return types. EP-8 owns topic-admin handle cleanup, including configuration failure before request submission. All changes to the binding module require regeneration and verification of `docs/bindings.md`.

Each child registers its tests in `test-integration/Main.hs` and `rdkafka-hs.cabal` as needed, using distinct test modules and additive edits. EP-9 introduces any shared weak-reference helper in `test-integration/Kafka/CallbackTestSupport.hs`; EP-11 may reuse or introduce that helper if implemented first. A weak reference observes whether captured state can be collected without keeping it alive itself. Tests must prove both retention while a callback is needed and collection after release, with bounded garbage-collection retries and helper functions that prevent accidental stack retention.

EP-11 must update ADR-1 with actual ownership and finalizer ordering when implemented. EP-9 must document precedence between global and explicit topic properties. Promote that precedence to an ADR only if it becomes a distinct lasting project contract; do not describe an unimplemented ownership design as already accepted. Other children distill any new durable constraints under the existing profiled ADR workflow.

The coordinator owns final reconciliation: all six finding IDs map to accepted tests; changelog and Haddocks describe the repaired behavior; source-distribution builds include all needed sources; public API compatibility and shutdown behavior remain intact. From the repository root, run:

```bash
nix develop -c cabal build all -f examples -f integration
nix develop -c cabal test rdkafka-hs-test --test-show-details=direct
nix develop -c just test-it
nix develop -c cabal check
nix develop -c cabal sdist
nix develop -c just check
```

Build and unit-test the emitted tarball from a fresh temporary extraction under the same development environment; follow the concrete candidate validation recipe in `docs/plans/6-cut-the-first-rdkafka-hs-release-and-migration-guide.md` without its publication steps. Record the source revision, linked librdkafka version, actual test counts, and tarball-build result here. A candidate source change invalidates affected evidence. Complete the ADR distillation pass and leave a handoff suitable for the separate release workflow; do not move tags, upload, or mark the repair intention complete merely because these planning documents exist.


## Progress


Planning complete; all five child plans are Not Started. There are no blocked children. Implementation and combined candidate validation remain outstanding.

- [ ] Combined candidate satisfies all six regressions, complete suites, source-distribution build, documentation checks, and lifecycle/API compatibility review.


## Surprises & Discoveries


The existing suites pass despite all six defects, so their results alone cannot establish release readiness. The review's queue-full probe retained callback state after a major garbage collection; a separate probe retained an error callback's state after explicit consumer destruction. These are different ownership defects and need different negative controls.

Producer configuration requires merging rather than blindly installing an empty topic configuration: librdkafka's default-topic replacement can overwrite topic-level settings already supplied globally. Preserve existing `extraProp` behavior when restoring `sendTimeout` and `topicCompression`.


## Decision Log


2026-10-23: Group producer settings and per-message callback ownership in EP-9; isolate persistent native callback ownership in EP-11. This keeps setup/enqueue regression fixtures together while making client teardown risk explicit.

2026-10-23: Preserve public API shapes and existing lifecycle semantics; fix behavior and ownership internally. Publication is a separate task because the user requested repair planning and existing tags already exist.

2026-10-23: Link this MasterPlan and all five children to the newly created personal Rei intention under the existing project root. Do not reuse the earlier fork initiative's intention or create five additional intentions without a request.


## Outcomes & Retrospective


Not implemented. Fill in accepted outcomes and remaining limitations after the combined candidate gate, including durable ADR updates.
