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
  revisions:
    - model: "gpt-6-astra"
      harness: "codex-cli"
      at: 2026-10-24T04:58:24Z
      mode: "update"
      note: "Expand acceptance to close public-contract, failure-path, concurrency, ownership, and compatibility coverage gaps; add EP-13 prerequisite and coordinated evidence gates."
    - model: "gpt-6.1-sol"
      harness: "codex-cli"
      at: 2026-10-24T07:53:34Z
      mode: "implement"
      note: "Begin producer setting and per-message callback regressions after accepted EP-13; retain EP-8 pending inherited seek investigation."
---

# Honor producer settings and release rejected callbacks


This ExecPlan is a living document. Keep Progress, Surprises & Discoveries, Decision Log, and Outcomes & Retrospective current.


## Purpose / Big Picture


Fix F2 and F4: producer timeout/compression/topic properties affect actual delivery, and an immediate enqueue failure releases the per-message callback and its captured data. Successful messages still retain their callback until a delivery report is served and invoke it once.


## Progress


- [x] (2026-10-24) M1: Effective native-client configuration and actual timeout/codec delivery honor explicit topic properties while preserving global settings and defined precedence; dropping settings or reversing native precedence fails compiled regressions.
- [x] (2026-10-24) M2: Repeated queue-full rejection releases captures; accepted, expired and purged callbacks retain/release correctly, including observed cancellation/transfer races and isolated throwing callbacks.

- [x] (2026-10-24) M3: Sixty-one focused cases cover producer data/configuration boundaries, native errors, delivery/purge outcomes and observed ownership races. Shared generated composition laws and callback-order observations pass, including four shrunk/replayed semantic failures.

Complete. F2 and F4 are repaired with effective native-client configuration, conditional per-message ownership, exception containment, exact byte preservation and native error-code propagation. Sixty-one focused cases and thirteen compiled producer mutations pass. EP-14's shared adapter adds six generated pure laws, a fixed edge case and a generated native callback-order property; all four reversed-bias mutations shrink and replay their failure tokens. Seed `20261024` passes 1,000 cases per pure law and 1,000 native callback programs.

Current normal and instrumented validation passes 21 unit and 244 integration tests (108.03 and 105.84 seconds), plus 18 tooling controls, strict ADR validation, Cabal/binding checks and Nix checks. The [producer manifest](../testing/producer-evidence.json) and [property manifest](../testing/producer-properties-evidence.json) record fingerprint `603e828f9aca4c20cfe867950e50ed5c106ff74de7db07124e960db07890cd9a`, resolved test dependencies, commands, actual native/broker versions and artifact hashes. Persistent native callback registrations remain EP-11; shared configuration C-string validation remains an explicit EP-14 row; native heap diagnostics and compatibility/authentication remain EP-15. Those open obligations still block the master gate.


## Surprises & Discoveries


At baseline, `sendTimeout (Timeout 1000)` against `127.0.0.1:1` leaves a message pending after a three-second flush, while `extraProp "message.timeout.ms" "1000"` expires it. Filling `queue.buffering.max.messages=1` and submitting a second message returns `RdKafkaRespErrQueueFull` but retains that second callback's captured IORef after major GC. F2 originated in retained commit `aa1c2c69` (2021), and F4 in `f81bec4` (2020).


The boundary matrix exposed additional inherited data violations: empty ByteStrings can have a null backing pointer and were encoded as absent values, embedded NUL topic/header names were truncated, and oversized Haskell partition integers wrapped into native values. Empty bytes now receive a non-null temporary pointer; NUL names and values outside C integer representation return an immediate `KafkaBadSpecification`. The prior code accepted all four invalid-input probes.

A throwing global delivery callback terminated an isolated process before the per-message callback ran. Exception handling now stays within Haskell threads, retaining synchronous global invocation and forked per-message execution. [ADR-13](../adr/delivery-callbacks-transfer-once-and-keep-exceptions-in-haskell.md) records the policy, including asynchronous exceptions during a global callback.

The mask-removal mutation initially survived cancellation after accepted enqueue because `bracket` itself masks acquisition. The rejected-enqueue cancellation window must also be tested: interrupting before rejection cleanup can retain its root. Premature accepted-root release aborts GHC before buffered test output, so the safety probe records actual native enqueue acceptance before accepting an isolated abort as mutation evidence.

The final failure audit reproduced a gap in the initial repair: a native enum-decoding exception occurred before rejection-root release. A contradictory non-null success-code error object also returned success, and valid native `UNKNOWN` errors were incorrectly replaced by system `errno`. The native nullable result pointer now decides ownership before decoding; all three new controls fail before the correction and pass afterward. Sixty-one focused producer cases pass, including zero/infinite and maximum native timeout values, excessive timeout rejection and alias precedence. Final evidence is being refreshed.

## Decision Log


2026-10-24: Keep configuration precedence and callback ownership as separate durable decisions, [ADR-12](../adr/producer-settings-merge-explicit-topic-keys-after-global-keys.md) and [ADR-13](../adr/delivery-callbacks-transfer-once-and-keep-exceptions-in-haskell.md). Native mock protocol responses provide controlled in-flight purge windows; they do not substitute for real-broker codec/configuration tests or EP-15 authentication and native diagnostics.

2026-10-24 revision: The user expanded acceptance to close coverage gaps. Add the complete behavioral/ownership matrix for this workstream, depend on EP-13's reliable fixtures, and require evidence per coverage-inventory row. Existing milestones remain required.

2026-10-23: Preserve global producer properties and apply explicit topic properties afterward, so explicit topic values win on duplicate keys without discarding unrelated global values. Preserve the per-message callback API; establish exception-safe ownership transfer at successful enqueue.


## Outcomes & Retrospective


F2 and F4 are complete. Explicit topic keys affect the producing native client without replacing unrelated global settings. NULL native error pointers transfer callback ownership; rejected roots release before decoding, including conversion exceptions. Valid UNKNOWN error-object codes remain native errors, and contradictory non-null success-code objects reject. Accepted callbacks survive cancellation and concurrent polling, and throwing global callbacks cannot bypass per-message execution or root release. Empty bytes remain distinct from absence. The accepted evidence includes 61 focused cases, thirteen producer mutations, generated/replayed composition laws and complete normal/HPC runs. ADR-12 and ADR-13 capture the lasting precedence and ownership/exception rules. EP-14 and EP-15 retain the explicitly separated broader obligations.


## Context and Orientation


`newProducer` in `src/Kafka/Producer.hs` builds `tc` from `ppTopicProps` but creates the native client from `kc'` only. The `produceva` path addresses topics by name, so `tc` is never used for producing. Helpers in `src/Kafka/Producer/ProducerProperties.hs` put `sendTimeout`, `topicCompression`, and `extraTopicProps` into that unused map. `src/Kafka/Dump.hs` exposes configuration inspection; retain its supported interfaces.

`produceMessage'` allocates a `StablePtr`, a runtime root retaining the Haskell callback while C stores its address. `src/Kafka/Producer/Callbacks.hs` frees it when processing a delivery report. Immediate enqueue failures get no delivery report, so they need immediate release. These pointers are distinct from the native callback function registrations addressed by EP-11. Add `test-integration/Kafka/Integration/ProducerRegressionTest.hs` and reuse EP-13's `test-integration/Kafka/CallbackTestSupport.hs` for bounded weak-reference checks.

This is the Haskell package `rdkafka-hs`, reviewed at commit `9c41b3d` (version `1.0.0.1`). The C library librdkafka performs the native work; `src/Kafka/Internal/RdKafka.chs` generates Haskell bindings through c2hs. Use the project Nix development shell: plain Cabal in the review environment could not find the C library. [ADR-4](../adr/toolchain-floor-is-ghc-9-12-and-librdkafka-2-15.md) retains GHC 9.12 and librdkafka 2.15. No dependency upgrade is required.

[ADR-5](../adr/integration-tests-run-against-a-project-local-redpanda.md) requires the private broker at `127.0.0.1:39092`, random run-prefixed topics/groups, and cleanup limited to that run. Existing fixtures are in `test-integration/Kafka/TestEnv.hs`; register new modules in `test-integration/Main.hs` and the integration suite's `other-modules` in `rdkafka-hs.cabal`. Never use the shared port 9092. The baseline passed 14 unit and 116 integration tests, so add a regression that distinguishes this defect from the baseline.

Resolve dependency sources with Mori before changing bindings. The source project is `mori://confluentinc/librdkafka`; its project-relative `librdkafka/src/rdkafka.h` describes ownership, with implementation in the adjacent C files (source-file artifact URIs pending). Discover its location with the commands below and inspect only the scoped checkout, never `/nix/store` or filesystem root. Keep existing pins; any proposed bound change requires checking the authoritative registry and upstream release tags.


## Plan of Work


M1 validates explicit topic properties using the existing topic configuration path, then applies those topic-level keys to the global native configuration before `newRdKafkaT`. The existing `setAllKafkaConfValues` helper can apply only the explicit topic map after `ppKafkaProps`; librdkafka accepts topic-level keys on global configuration. Do not blindly install a fresh default-topic object: that replaces settings already supplied through global `extraProps`. Keep the retained `TopicConf` field and existing public shapes. Test defaults, only-global settings, only-topic settings, disjoint settings, and duplicate-key precedence. Verify compression through effective native configuration plus a successful delivery, and timeout through actual delivery failure, not merely the unused `tc` dump.

M2 masks asynchronous exceptions around callback allocation and the enqueue ownership decision. On immediate failure, free the StablePtr exactly once; on success, ownership passes to the delivery callback. Do not use an unconditional bracket that frees a successful enqueue, or an `onException` covering work after ownership has passed. Account for delivery reporting from another polling thread: successful enqueue must never lead to the caller freeing the pointer. Review the delivery handler's release boundary so a throwing global callback cannot bypass release of the per-message StablePtr; retain existing callback execution policy without allowing cleanup to unwind unsafely through C. Document any existing policy limitation rather than silently changing it.

Build the weak-reference fixture in a separate non-inlined helper: create an IORef, a weak reference to it, and a callback that mutates that IORef; return only the weak reference after enqueueing. A bounded GC loop must show retention for an accepted pending message and collection after rejected enqueue or a served delivery report. Wait for forked message callbacks to finish before testing collection. Include a control object with no callback root so optimizer or stack retention cannot manufacture a passing test. Add changelog and Haddock explanations for setting precedence.


M3 exercises the complete existing producer contract around the repaired paths. Round-trip absent versus empty keys/values, tombstones, binary payloads with zero bytes, duplicate/empty headers, specified versus unassigned partitions, and supported compression values. Cover oversized records, known invalid partitions, queue-full rejection, no pending messages, successful/expired delivery, purge flags separately and in combination, bounded flush timeout followed by recovery, and repeated flush/close. For global and explicit topic properties test empty identity, right-biased composition, unrelated-key preservation, invalid keys/values, and callback replacement order. All delivery tests assert exact record identity and report counts.

Schedule producer concurrency with barriers: accepted enqueue while another thread serves its delivery report, exception before ownership transfer, immediate failure after allocation, and cleanup while a report callback is running. Test both a throwing global delivery callback and a throwing per-message callback in an isolated process if necessary; define and assert the library's supported exception policy without allowing exceptions to unwind through foreign frames or silently leak resources. Repeatedly injecting cancellation without observing the transfer boundary is not sufficient. Add replayable generated property cases for producer property composition through EP-14's shared harness; this child owns their behavior requirements.

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


The producer rows in `docs/testing/coverage-matrix.json` must include accepted-message and immediate-failure ownership, every meaningful purge/flush outcome, and byte-exact record/report observations. Include mutations that drop topic settings, omit rejection cleanup, and free accepted callbacks too early; each must make a focused test fail or the isolated safety probe fail. Test weak-reference lifetimes under the normal optimized build as well as coverage instrumentation; report completion of forked callbacks before asserting collection. Real authentication failures/recovery are owned by EP-15, not satisfied by calling `setSaslCredentials` against a plaintext broker.

All new cases use EP-13's failure-preserving fixtures and are recorded with exact test names and candidate evidence in the coverage inventory. No known failing case may be marked expected-failure to claim completion.

## Idempotence and Recovery


These changes require no data migration. Re-running tests uses fresh run prefixes and must delete only resources created by that run. Use bounded waits and `bracket`/`finally` for fixture cleanup; a failed assertion must not leave a producer flushing indefinitely. Do not reset the shared broker or overwrite unrelated edits. If a proof fails, leave the milestone unfinished, record the failing behavior, and fix the implementation or fixture before broadening the test matrix. Do not publish or move release tags.


## Interfaces and Dependencies


Hard dependency: `docs/plans/13-make-coverage-evidence-and-test-fixtures-reliable.md` must be Complete; its trustworthy fixtures and coverage inventory are prerequisites. There is an integration dependency with `docs/plans/11-own-native-callbacks-through-client-teardown.md`: this plan owns effective producer configuration and per-message StablePtr transfer; that plan owns FunPtr registration and client finalizers. Neither may release the other's pointers. EP-11 has a soft dependency on this plan's tests and must rerun the accepted-message/delivery path after constructor changes. Public producer function signatures, `ProducerProperties`, and close/flush semantics remain unchanged. Do not introduce a new producer destruction API.
