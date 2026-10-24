---
id: 9
slug: honor-producer-settings-and-release-rejected-callbacks
title: "Honor producer settings and release rejected callbacks"
kind: exec-plan
created_at: 2026-10-24T04:38:50Z
intention: "intention_01m5p37ej8e3h92gkf2wp9j6qa"
master_plan: "docs/masterplans/2-fix-inherited-release-review-defects.md"
provenance:
  created_by:
    model: "gpt-6-astra"
    harness: "codex-cli"
    at: 2026-10-24T04:38:50Z
---

# Honor producer settings and release rejected callbacks


This ExecPlan is a living document. Keep Progress, Surprises & Discoveries, Decision Log, and Outcomes & Retrospective current.


## Purpose / Big Picture


Fix F2 and F4: producer timeout/compression/topic properties affect actual delivery, and an immediate enqueue failure releases the per-message callback and its captured data. Successful messages still retain their callback until a delivery report is served and invoke it once.


## Progress


- [ ] M1: Effective producer settings honor topic properties while preserving global settings and defined precedence.
- [ ] M2: Queue-full rejection releases captured callback state while successful/purged messages retain and release their callbacks correctly.

Not started; this document records the implementation contract, not completed fixes.


## Surprises & Discoveries


At baseline, `sendTimeout (Timeout 1000)` against `127.0.0.1:1` leaves a message pending after a three-second flush, while `extraProp "message.timeout.ms" "1000"` expires it. Filling `queue.buffering.max.messages=1` and submitting a second message returns `RdKafkaRespErrQueueFull` but retains that second callback's captured IORef after major GC. F2 originated in retained commit `aa1c2c69` (2021), and F4 in `f81bec4` (2020).


## Decision Log


2026-10-23: Preserve global producer properties and apply explicit topic properties afterward, so explicit topic values win on duplicate keys without discarding unrelated global values. Preserve the per-message callback API; establish exception-safe ownership transfer at successful enqueue.


## Outcomes & Retrospective


Not implemented. Record accepted evidence, remaining limitations, and durable lessons here at completion; update relevant ADRs before marking Complete.


## Context and Orientation


`newProducer` in `src/Kafka/Producer.hs` builds `tc` from `ppTopicProps` but creates the native client from `kc'` only. The `produceva` path addresses topics by name, so `tc` is never used for producing. Helpers in `src/Kafka/Producer/ProducerProperties.hs` put `sendTimeout`, `topicCompression`, and `extraTopicProps` into that unused map. `src/Kafka/Dump.hs` exposes configuration inspection; retain its supported interfaces.

`produceMessage'` allocates a `StablePtr`, a runtime root retaining the Haskell callback while C stores its address. `src/Kafka/Producer/Callbacks.hs` frees it when processing a delivery report. Immediate enqueue failures get no delivery report, so they need immediate release. These pointers are distinct from the native callback function registrations addressed by EP-11. Add `test-integration/Kafka/Integration/ProducerRegressionTest.hs` and, if useful, `test-integration/Kafka/CallbackTestSupport.hs` for bounded weak-reference checks.

This is the Haskell package `rdkafka-hs`, reviewed at commit `9c41b3d` (version `1.0.0.1`). The C library librdkafka performs the native work; `src/Kafka/Internal/RdKafka.chs` generates Haskell bindings through c2hs. Use the project Nix development shell: plain Cabal in the review environment could not find the C library. [ADR-4](../adr/toolchain-floor-is-ghc-9-12-and-librdkafka-2-15.md) retains GHC 9.12 and librdkafka 2.15. No dependency upgrade is required.

[ADR-5](../adr/integration-tests-run-against-a-project-local-redpanda.md) requires the private broker at `127.0.0.1:39092`, random run-prefixed topics/groups, and cleanup limited to that run. Existing fixtures are in `test-integration/Kafka/TestEnv.hs`; register new modules in `test-integration/Main.hs` and the integration suite's `other-modules` in `rdkafka-hs.cabal`. Never use the shared port 9092. The baseline passed 14 unit and 116 integration tests, so add a regression that distinguishes this defect from the baseline.

Resolve dependency sources with Mori before changing bindings. The source project is `mori://confluentinc/librdkafka`; its project-relative `librdkafka/src/rdkafka.h` describes ownership, with implementation in the adjacent C files (source-file artifact URIs pending). Discover its location with the commands below and inspect only the scoped checkout, never `/nix/store` or filesystem root. Keep existing pins; any proposed bound change requires checking the authoritative registry and upstream release tags.


## Plan of Work


M1 validates explicit topic properties using the existing topic configuration path, then applies those topic-level keys to the global native configuration before `newRdKafkaT`. The existing `setAllKafkaConfValues` helper can apply only the explicit topic map after `ppKafkaProps`; librdkafka accepts topic-level keys on global configuration. Do not blindly install a fresh default-topic object: that replaces settings already supplied through global `extraProps`. Keep the retained `TopicConf` field and existing public shapes. Test defaults, only-global settings, only-topic settings, disjoint settings, and duplicate-key precedence. Verify compression through effective native configuration plus a successful delivery, and timeout through actual delivery failure, not merely the unused `tc` dump.

M2 masks asynchronous exceptions around callback allocation and the enqueue ownership decision. On immediate failure, free the StablePtr exactly once; on success, ownership passes to the delivery callback. Do not use an unconditional bracket that frees a successful enqueue, or an `onException` covering work after ownership has passed. Account for delivery reporting from another polling thread: successful enqueue must never lead to the caller freeing the pointer. Review the delivery handler's release boundary so a throwing global callback cannot bypass release of the per-message StablePtr; retain existing callback execution policy without allowing cleanup to unwind unsafely through C. Document any existing policy limitation rather than silently changing it.

Build the weak-reference fixture in a separate non-inlined helper: create an IORef, a weak reference to it, and a callback that mutates that IORef; return only the weak reference after enqueueing. A bounded GC loop must show retention for an accepted pending message and collection after rejected enqueue or a served delivery report. Wait for forked message callbacks to finish before testing collection. Include a control object with no callback root so optimizer or stack retention cannot manufacture a passing test. Add changelog and Haddock explanations for setting precedence.


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

Run the new focused group:

```bash
just test-it '--test-options=-p /producer-regressions/'
```

For the unavailable-broker fixture, use loopback port 1, explicit short time bounds, and purge in cleanup before closing. For queue-full rejection set `queue.buffering.max.messages=1`, enqueue one pending message, and assert the second enqueue actually returns `ImmediateError` with `RdKafkaRespErrQueueFull` before interpreting collection.


## Validation and Acceptance


The one-second topic timeout produces a delivery failure and empties the queue within a generous bounded window; the same global setting still works. Explicit topic values override only matching global values, and unrelated global properties survive. A compression setting appears in effective client/topic configuration used by producing, and a record round-trips successfully. A rejected callback is collectable and never invoked; an accepted callback remains alive until its delivery report, executes once, and then becomes collectable, including the purge case. Repeat a bounded queue-full batch to detect retention, and demonstrate that the baseline fails the negative-path assertions. Existing headers, lifecycle, delivery, and transaction tests remain green.


## Idempotence and Recovery


These changes require no data migration. Re-running tests uses fresh run prefixes and must delete only resources created by that run. Use bounded waits and `bracket`/`finally` for fixture cleanup; a failed assertion must not leave a producer flushing indefinitely. Do not reset the shared broker or overwrite unrelated edits. If a proof fails, leave the milestone unfinished, record the failing behavior, and fix the implementation or fixture before broadening the test matrix. Do not publish or move release tags.


## Interfaces and Dependencies


No hard dependencies. There is an integration dependency with `docs/plans/11-own-native-callbacks-through-client-teardown.md`: this plan owns effective producer configuration and per-message StablePtr transfer; that plan owns FunPtr registration and client finalizers. Neither may release the other's pointers. EP-11 has a soft dependency on this plan's tests and must rerun the accepted-message/delivery path after constructor changes. Public producer function signatures, `ProducerProperties`, and close/flush semantics remain unchanged. Do not introduce a new producer destruction API.
