---
id: 11
slug: own-native-callbacks-through-client-teardown
title: "Own native callbacks through client teardown"
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

# Own native callbacks through client teardown


This ExecPlan is a living document. Keep Progress, Surprises & Discoveries, Decision Log, and Outcomes & Retrospective current.


## Purpose / Big Picture


Fix F5: creating and discarding clients no longer permanently retains Haskell callback closures. A callback remains callable while librdkafka can invoke it, including during teardown, and its function pointer is freed once after the last native owner finishes. Explicit consumer destruction and producer garbage-collector finalization must both release registrations.


## Progress


- [ ] M1: A representative native callback owner proves retention during client lifetime and collection after teardown without changing public lifecycle semantics.
- [ ] M2: Every installed callback kind, replacement, configuration failure, and client-creation failure follows the same complete ownership path.
- [ ] M3: Explicit/repeated/concurrent consumer destruction and producer finalization pass callback retention and existing lifecycle regressions.

- [ ] M4: Every registration/transfer/teardown path and the meaningful interruption windows have deterministic lifetime evidence.

Not started; this document records the implementation contract, not completed fixes.


## Surprises & Discoveries


At baseline an error callback captures an IORef; after `destroyConsumer` and major GC a weak reference still observes that IORef. The setters allocate wrappers with `foreign import ccall "wrapper"` and never call `freeHaskellFunPtr`. The error setter already has this defect in retained commit `022a9699` (2017). Default rebalance callbacks capture KafkaConf, so attaching a finalizer only to that same configuration risks a retention cycle.


## Decision Log


2026-10-24 revision: The user expanded acceptance to close coverage gaps. Add the complete behavioral/ownership matrix for this workstream, depend on EP-13's reliable fixtures, and require evidence per coverage-inventory row. Existing milestones remain required.

2026-10-23: Preserve ADR-1's single native-client deallocator and separate close versus destroy semantics. Own callback registrations through a client-independent cleanup token that the native-client finalizer can release only after native destruction, with explicit cleanup for construction failures. Do not make `closeProducer` suddenly destroy the client.


## Outcomes & Retrospective


Not implemented. Record accepted evidence, remaining limitations, and durable lessons here at completion; update relevant ADRs before marking Complete.


## Context and Orientation


`src/Kafka/Internal/RdKafka.chs` allocates FunPtrs in the `rdKafkaConfSet*Cb` setters. A FunPtr is executable bridge code allowing C to call a Haskell closure; the Haskell runtime roots that closure until the bridge is freed. `newRdKafkaT` duplicates native configuration with `rdKafkaConfDup` and registers one callback-safe concurrent finalizer that calls `rd_kafka_destroy_flags`. Duplication copies callback addresses, not independent Haskell registrations. `src/Kafka/Internal/Setup.hs` defines `KafkaConf`, `Callback`, queue references, and lifecycle locks. Callback installers live in `src/Kafka/Callbacks.hs`, `src/Kafka/Consumer/Callbacks.hs`, and `src/Kafka/Producer/Callbacks.hs`; constructors are in `src/Kafka/Consumer.hs` and `src/Kafka/Producer.hs`.

[ADR-1](../adr/consumer-queue-and-teardown-ownership.md) is binding: `closeConsumer` gracefully closes without destroying; `destroyConsumer` synchronously finalizes once and waits for active polling; producer close currently flushes only, with destruction later by GC. Preserve those contracts. A `KafkaConsumer` supplied to callbacks wraps a borrowed pointer without a deallocator; do not accidentally transfer ownership to it. Add `test-integration/Kafka/Integration/CallbackLifetimeTest.hs` and reuse EP-13's `test-integration/Kafka/CallbackTestSupport.hs` for collection observations.

This is the Haskell package `rdkafka-hs`, reviewed at commit `9c41b3d` (version `1.0.0.1`). The C library librdkafka performs the native work; `src/Kafka/Internal/RdKafka.chs` generates Haskell bindings through c2hs. Use the project Nix development shell: plain Cabal in the review environment could not find the C library. [ADR-4](../adr/toolchain-floor-is-ghc-9-12-and-librdkafka-2-15.md) retains GHC 9.12 and librdkafka 2.15. No dependency upgrade is required.

[ADR-5](../adr/integration-tests-run-against-a-project-local-redpanda.md) requires the private broker at `127.0.0.1:39092`, random run-prefixed topics/groups, and cleanup limited to that run. Existing fixtures are in `test-integration/Kafka/TestEnv.hs`; register new modules in `test-integration/Main.hs` and the integration suite's `other-modules` in `rdkafka-hs.cabal`. Never use the shared port 9092. The baseline passed 14 unit and 116 integration tests, so add a regression that distinguishes this defect from the baseline.

Resolve dependency sources with Mori before changing bindings. The source project is `mori://confluentinc/librdkafka`; its project-relative `librdkafka/src/rdkafka.h` describes ownership, with implementation in the adjacent C files (source-file artifact URIs pending). Discover its location with the commands below and inspect only the scoped checkout, never `/nix/store` or filesystem root. Keep existing pins; any proposed bound change requires checking the authoritative registry and upstream release tags.


## Plan of Work


M1 is a bounded implementation prototype with an explicit acceptance gate before all setters are migrated. Introduce an internal ownership token/registry that records allocated callback pointers and can release them idempotently. Keep its implementation below `Kafka.Internal.Setup` or in a new internal module to avoid a Setup/RdKafka import cycle. Tie cleanup to the native client's existing finalizer, after `rd_kafka_destroy_flags` has returned, and to explicit pre-client failure cleanup. Do not use the callback-captured KafkaConf as the sole object whose GC triggers cleanup; its FunPtr roots would keep that object alive. Do not capture the owning client ForeignPtr in a registered callback or its cleanup token. Prove a real producer delivery callback and a consumer error callback remain usable while live, then become collectable after their correct destruction boundary. Promote the design only if this works under GC and existing teardown tests; otherwise revise the ownership graph before migrating more callbacks.

M2 enumerates every wrapper allocation and gives each a cleanup path, including the default producer delivery callback replaced by a user callback and the async consumer rebalance default replaced by another installer. Prefer retaining replaced registrations until the construction/client owner is safely released; early release is permissible only with proof no duplicated native configuration can still reference them. Cover partially installed callback lists, invalid topic configuration after callbacks are registered, and `rd_kafka_new` failure. On native creation failure, free the duplicated configuration if librdkafka left ownership with the caller, then release registrations only after all usable native references are gone. The retained original configuration used for dumps must not be reused to construct a client with stale pointers after release. Update internal signatures as needed while preserving exposed record/function shapes.

M3 integrates registry release into the one actual native-client finalizer and proves it is once-only across explicit destruction and later GC. No callback pointer can be freed before the destructor stops native threads and callbacks. Handle asynchronous exceptions without losing a cleanup action or marking cleanup complete prematurely; never use an unbounded uninterruptible mask around native shutdown. Preserve the existing repeated/concurrent/interrupted destroy checks. For a producer, flush, release application references, and use bounded GC/finalizer waits; close alone is not the release boundary. Existing consumer callback borrowed wrappers must not finalize a native client themselves. Update ADR-1 with the proven owner graph and shutdown ordering, following `agents/skills/exec-plan/ADR.md`, and document the inherited fix in the changelog.


M4 closes lifetime and scheduling gaps. Enumerate each wrapper allocation, original/duplicated configuration, owner transfer, registration replacement, client constructor failure, finalizer, and explicit release path in the ownership inventory. Test every publicly installable callback kind, multiple registered callbacks, overwritten defaults, partial callback-list installation, and callbacks that capture configuration state. Native callbacks during shutdown must be observed using a counted barrier, not assumed from lack of a crash.

Replace the weak interrupted-destroy evidence with deterministic stage controls: interruption while waiting for the callback lock, after callback polling stops, after queue release, and around finalizer handoff. Observe arrival at the stage before injecting cancellation, then require a retry to finish cleanup once. For concurrent destroy callers, hold a callback/native shutdown stage open, prove both callers wait, release it, and prove neither returns before teardown completes. Also exercise explicit destroy followed by GC, GC-only native finalization after the application releases an appropriately closed client, and failed construction before any client exists. Respect caller serialization preconditions: a deliberately invalid use-after-destroy call is not a required safe public operation. Private fault hooks must not become public API or affect normal builds. Run GC/ownership cases under both a normal optimized build and coverage instrumentation, including noncaptured and intentionally retained controls.

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

Run these focused groups after registering the new tests:

```bash
just test-it '--test-options=-p /callback-lifetime/'
just test-it '--test-options=-p /destroy/'
just test-it '--test-options=-p /producer-regressions/'
```

The last command requires the tests from `docs/plans/9-honor-producer-settings-and-release-rejected-callbacks.md`; if that child is not yet implemented, add an equivalent accepted-message delivery probe now and rerun its full group during integration. Enumerate all allocation sites with:

```bash
rg -n 'foreign import.*wrapper|mk.*Callback|freeHaskellFunPtr|newRdKafkaT' src/Kafka
```

After updating ADR-1 run strict profile/log validation as described in `agents/skills/exec-plan/ADR.md` and `just docs-check`.


## Validation and Acceptance


A weak reference to a callback-captured IORef stays live while the native client owns the callback, and the callback demonstrably runs. It becomes dead after explicit consumer destruction or actual producer finalization, including when the user retains the destroyed consumer's Haskell wrapper. Finalizer completion is observed with bounded waits, not an assumption that one GC is enough. A noncaptured control object is collectable. The original code fails this collection test.

Overridden/default callbacks, construction failure after registration, invalid native-client configuration, and repeated client creation/destruction do not accumulate live registrations. Use a counted allocator/releaser fixture for the internal registry to assert each allocated wrapper has exactly one release, including exception injection; black-box weak references additionally prove real C/Haskell integration. Native callbacks during shutdown stay valid and no callback runs after destruction returns. All existing destroy, queue-event, fatal-consumer, producer-delivery, and read-committed tests remain green. Do not use aggregate RSS alone as leak evidence.


All callback allocation sites must be reconciled with counted exactly-once release or a documented non-allocating/unreachable classification. Tests must fail if registration release is removed, if release is moved before native shutdown, or if lifecycle state is published before teardown completes. Weak references alone cannot prove native callbacks cannot use a freed address; combine live callback observations, a shutdown barrier, release counters, and a native invalid-access diagnostic on the supported diagnostic platform. Retained destroyed wrappers and captured configuration must not keep released callback closures alive. The old immediate `killThread` test can remain as a smoke test but cannot satisfy deterministic interruption acceptance.

All new cases use EP-13's failure-preserving fixtures and are recorded with exact test names and candidate evidence in the coverage inventory. No known failing case may be marked expected-failure to claim completion.

## Idempotence and Recovery


These changes require no data migration. Re-running tests uses fresh run prefixes and must delete only resources created by that run. Use bounded waits and `bracket`/`finally` for fixture cleanup; a failed assertion must not leave a producer flushing indefinitely. Do not reset the shared broker or overwrite unrelated edits. If a proof fails, leave the milestone unfinished, record the failing behavior, and fix the implementation or fixture before broadening the test matrix. Do not publish or move release tags.


## Interfaces and Dependencies


Hard dependency: `docs/plans/13-make-coverage-evidence-and-test-fixtures-reliable.md` must be Complete; its trustworthy fixtures and coverage inventory are prerequisites. Soft dependency on `docs/plans/9-honor-producer-settings-and-release-rejected-callbacks.md` for its producer tests. This child owns KafkaConf/client lifetime internals and all persistent callback registrations; EP-9 owns per-message StablePtrs. Do not add new exported constructor fields or new public destroy operations as an incidental fix. Any new internal module must be in the library's `other-modules`, and its source must ship in `cabal sdist`. The cross-plan gate reruns EP-9 after constructor integration.
