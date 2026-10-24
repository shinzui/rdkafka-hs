---
id: 12
slug: release-transaction-error-objects
title: "Release transaction error objects"
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

# Release transaction error objects


This ExecPlan is a living document. Keep Progress, Surprises & Discoveries, Decision Log, and Outcomes & Retrospective current.


## Purpose / Big Picture


Fix F6: failed or retried transaction initialization, begin, and abort operations release their native error objects while returning the same errors to callers. Successful transaction cycles and commit error flags retain their existing meaning.


## Progress


- [ ] M1: Init, begin, and abort preserve results while bracketing every returned native error object.
- [ ] M2: Repeated error paths have allocation-specific leak evidence and successful commit/abort cycles pass broker tests.

Not started; this document records the implementation contract, not completed fixes.


## Surprises & Discoveries


Retained commit `c58bc85` (September 2022) introduced the three wrappers that extract an error code and discard its pointer without destruction. `rdKafkaErrorTPtr` is a raw Ptr, not a ForeignPtr with a finalizer. The review confirmed the leak by source inspection; native allocation evidence remains a required implementation deliverable.


## Decision Log


2026-10-23: Keep transaction public return types unchanged. Use scoped destruction around error decoding, including exceptions; preserve null-as-success. Do not turn this repair into a redesign of transaction retry policy or public TxError types.


## Outcomes & Retrospective


Not implemented. Record accepted evidence, remaining limitations, and durable lessons here at completion; update relevant ADRs before marking Complete.


## Context and Orientation


`src/Kafka/Transaction.hs:initTransactions`, `beginTransaction`, and `abortTransaction` chain the native call into `rdKafkaErrorCode` and `handleProduceErrT`, never invoking `rdKafkaErrorDestroy`. The same module's `toTxError` reads fatal/retriable/abort-required flags for commit/send-offsets errors and manually destroys the object afterward. `src/Kafka/Internal/RdKafka.chs` declares the raw error pointer and C functions. In the native implementation both `rd_kafka_error_code(NULL)` and `rd_kafka_error_destroy(NULL)` are valid, so a single bracket can handle success and failure.

`test-integration/Kafka/Integration/ReadCommittedTest.hs` already establishes a successful transactional producer and commit against the private broker. Add `test-integration/Kafka/Integration/TransactionErrorTest.hs`; use fresh transactional IDs and bounded timeouts. [ADR-2](../adr/release-validation-before-publication.md) requires evidence against the candidate, not merely a corrected-looking source snippet.

This is the Haskell package `rdkafka-hs`, reviewed at commit `9c41b3d` (version `1.0.0.1`). The C library librdkafka performs the native work; `src/Kafka/Internal/RdKafka.chs` generates Haskell bindings through c2hs. Use the project Nix development shell: plain Cabal in the review environment could not find the C library. [ADR-4](../adr/toolchain-floor-is-ghc-9-12-and-librdkafka-2-15.md) retains GHC 9.12 and librdkafka 2.15. No dependency upgrade is required.

[ADR-5](../adr/integration-tests-run-against-a-project-local-redpanda.md) requires the private broker at `127.0.0.1:39092`, random run-prefixed topics/groups, and cleanup limited to that run. Existing fixtures are in `test-integration/Kafka/TestEnv.hs`; register new modules in `test-integration/Main.hs` and the integration suite's `other-modules` in `rdkafka-hs.cabal`. Never use the shared port 9092. The baseline passed 14 unit and 116 integration tests, so add a regression that distinguishes this defect from the baseline.

Resolve dependency sources with Mori before changing bindings. The source project is `mori://confluentinc/librdkafka`; its project-relative `librdkafka/src/rdkafka.h` describes ownership, with implementation in the adjacent C files (source-file artifact URIs pending). Discover its location with the commands below and inspect only the scoped checkout, never `/nix/store` or filesystem root. Keep existing pins; any proposed bound change requires checking the authoritative registry and upstream release tags.


## Plan of Work


M1 brackets native acquisition, decoding, and destruction for each of the three wrappers. Read/force the returned error information before freeing its backing C memory. If a shared internal decoder improves clarity, keep it private; do not create a broad resource framework. Apply the same scoped pattern to `toTxError` where needed so an exception while reading flags cannot leak an error object. Preserve all existing error classifications, retry hints, and null-success behavior. Keep consumer-group metadata cleanup in view if touching send-offsets acquisition, but do not add unrelated transaction features.

M2 adds errors from all three public entry points. A producer without `transactional.id` yields immediate configuration/state errors without a broker request; capture the exact supported-version codes and assert them. Also exercise a bounded initialization timeout against loopback port 1 with a transactional ID, plus successful initialization/begin/abort and initialization/begin/commit against the private broker. Ensure cleanup aborts an open transaction when necessary and purges pending messages before closing an unavailable-broker fixture.

For native memory proof, run an isolated test case named `transaction-error-leak-probe` that repeatedly invokes the three immediate-error paths on one nontransactional producer. Do not create a client for every iteration, which would confound callback/client leaks. Build with symbols and use macOS `leaks` with allocation stack logging, available in the reviewed environment. Compare baseline and repaired builds for allocations rooted in `rd_kafka_error_new`/`rd_kafka_error_new_v` from these wrapper paths; distinguish native client background allocations and unrelated known defects. If the allocator tool cannot symbolize or inspect the process, use a documented test-only instrumented native allocator/destructor counter around these returned error objects, with a known-leaking control. Never accept stable RSS or only passing return-value assertions as proof of destruction. Record exact commands, iteration count, and before/after attributed results. Add changelog coverage.


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

Run the focused group and build its native leak probe from the same source:

```bash
just test-it '--test-options=-p /transaction-errors/'
cabal build test:rdkafka-hs-integration -f integration
review_test_bin=$(cabal list-bin test:rdkafka-hs-integration -f integration)
MallocStackLogging=1 leaks --atExit -- "$review_test_bin" -p /transaction-error-leak-probe/
```

Run these inside the development shell after starting the private broker with `just redpanda-up`, since the integration executable performs a broker preflight even for the immediate-error case. `leaks` may report unrelated runtime allocations; inspect the attributed error-object stacks, record results, and retain an actual before/after negative control rather than treating the tool exit code as the sole assertion.


## Validation and Acceptance


Each immediate-error call returns the same Kafka error before and after the repair. Nontransactional init/begin/abort loops have no outstanding native error objects attributable to these calls after decoding; the baseline shows their allocation stacks or a positive counter imbalance. A timed-out initialization remains a timeout and releases its returned object. Valid commit and abort cycles return success, and existing read-committed tests pass. Commit/send-offsets flags are copied before release and remain correct; a scoped decoder releases once even if conversion throws. All native error ownership paths touched by this child are exception-safe.


## Idempotence and Recovery


These changes require no data migration. Re-running tests uses fresh run prefixes and must delete only resources created by that run. Use bounded waits and `bracket`/`finally` for fixture cleanup; a failed assertion must not leave a producer flushing indefinitely. Do not reset the shared broker or overwrite unrelated edits. If a proof fails, leave the milestone unfinished, record the failing behavior, and fix the implementation or fixture before broadening the test matrix. Do not publish or move release tags.


## Interfaces and Dependencies


No hard or soft dependency. This child owns transaction error conversion in `src/Kafka/Transaction.hs` and its new tests. EP-11 (`docs/plans/11-own-native-callbacks-through-client-teardown.md`) independently owns persistent callback/client resources; isolate allocation evidence so its baseline leaks cannot mask this fix. The `TxError` API and producer/client lifecycle semantics remain unchanged. The master coordinator owns full-candidate validation after all children complete.
