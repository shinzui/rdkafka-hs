---
id: 10
slug: report-per-partition-seek-failures
title: "Report per-partition seek failures"
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
      at: 2026-10-24T10:48:54Z
      mode: "implement"
      note: "Implement native partition error reporting and seek/rewind boundary regressions after accepted fixture milestone."
---

# Report per-partition seek failures


This ExecPlan is a living document. Keep Progress, Surprises & Discoveries, Decision Log, and Outcomes & Retrospective current.


## Purpose / Big Picture


Fix F3: synchronous `seekPartitions` returns an error when any requested partition fails to seek. Applications can distinguish a successful rewind from an unassigned or invalid partition without changing the existing result type.


## Progress


- [x] M1: Synchronous seeks return native per-partition errors, including mixed success/failure requests.
- [x] M2: Valid replay, zero-timeout initiation, top-level failures, and rewind behavior retain their documented contracts in both poll modes.

- [x] M3: Seek and rewind boundaries, partial success, offset sentinels, and assignment changes are verified in both callback modes.

Complete. The original false success reproduced in both callback modes (2/4 initial tests failed). Seventy-six focused cases verify ordered member failures, overall precedence, initiation-only pending semantics, duplicate entries, exact replay and rewind boundaries. Seven compiled semantic mutations fail their intended tests; the unchanged control passes.

The final candidate `e0efdb19089bfda33df08ddbc34c1a98a57c7a16430ce167addf2a2ff3c3fa14` passes 21 unit and 320 integration tests in normal (156.82 seconds) and instrumented (149.39 seconds) builds, 18 tooling controls, Nix checks, package checks and strict validation of 14 ADRs. See [seek evidence](../testing/seek-evidence.json). Public exports and types remain unchanged. The saved deprecated-seek failure remains EP-14; callback finalization remains EP-11.


## Surprises & Discoveries


The review called `seekPartitions` on a new assigned-only consumer with no assignment, requesting topic `never-assigned`, partition 42, and a one-second timeout. The result was `Nothing`, although no seek could occur. The behavior is inherited from retained commit `8d24891` (September 2022).

The supported native implementation treats a known but inactive partition differently from a wholly unknown topic: a synchronous seek returns `STATE` for the former, `UNKNOWN_PARTITION` for the latter. A zero-timeout seek can report the unknown-topic error immediately, but not a later native-thread rejection such as `STORED`. The first expanded fixture assumed both were unknown; six of 70 tests correctly rejected that assumption. The corrected suite distinguishes actual native phases. The three relevant seek/list functions in the Mori checkout at `122fc0c98562d086c3618bf376862632ae0808e1` are byte-identical to upstream release tag `v2.15.0` (`9a94e11452cdeb0a844db44ee5dd01ccbe17d3ab`); source locations belong to `mori://confluentinc/librdkafka` as named below.

Native duplicate requests receive replies through a first-match lookup, leaving a later duplicate marked `IN_PROGRESS`. Keep that residual error visible for nonzero timeouts rather than silently deduplicating the public input. Retained-log tests trim only the run-owned partition, verify broker low/high watermarks `(2,4)`, and prove reset to offset 2.


## Decision Log

2026-10-24 implementation: Read member errors in a narrow internal helper while retaining the list. Keep generic conversions and public types unchanged. [ADR-14](../adr/seek-reports-ordered-member-errors-and-preserves-initiation-semantics.md) records overall precedence, request ordering, partial success and initiation-only semantics.


2026-10-24 revision: The user expanded acceptance to close coverage gaps. Add the complete behavioral/ownership matrix for this workstream, depend on EP-13's reliable fixtures, and require evidence per coverage-inventory row. Existing milestones remain required.

2026-10-23: Preserve `m (Maybe KafkaError)`. A top-level error takes precedence; otherwise report the first non-success partition error in input order. A zero timeout promises initiation only, so it cannot promise final success of an asynchronous seek.


## Outcomes & Retrospective


F3 is repaired without a public type change. A native overall success no longer conceals member errors, zero timeout retains asynchronous initiation, and mixed failures do not imply rollback. Rewind resumes from the exact committed next offset and reports a real assignment change after lookup. Both owned inventory rows are tested. [ADR-14](../adr/seek-reports-ordered-member-errors-and-preserves-initiation-semantics.md) distills the durable result contract; transient fixture assumptions and before-fix failures remain documented above. Compatibility, native heap diagnostics and unrelated open contracts still block master completion.


## Context and Orientation


`src/Kafka/Consumer.hs:seekPartitions` converts input to a native list, calls `rdKafkaSeekPartitions`, destroys the returned top-level error object, and checks only that object's code. librdkafka separately writes each partition's `err` field in the supplied list. `src/Kafka/Internal/RdKafka.chs` already exposes `err'RdKafkaTopicPartitionT` and list element/count fields. `src/Kafka/Consumer/Convert.hs` drops those errors when converting to public `TopicPartition`, whose shape must remain unchanged. `rewindConsumer` uses `seekPartitions`, so this omission can conceal recovery failures.

Add `test-integration/Kafka/Integration/SeekPartitionsTest.hs`. Existing `ConsumerTest.hs` mostly exercises the deprecated `seek`, which does not cover this wrapper. [ADR-1](../adr/consumer-queue-and-teardown-ownership.md) requires working queue routing and consumer teardown in both synchronous and asynchronous callback poll modes; those modes determine who polls callbacks, not whether a seek timeout is zero.

This is the Haskell package `rdkafka-hs`, reviewed at commit `9c41b3d` (version `1.0.0.1`). The C library librdkafka performs the native work; `src/Kafka/Internal/RdKafka.chs` generates Haskell bindings through c2hs. Use the project Nix development shell: plain Cabal in the review environment could not find the C library. [ADR-4](../adr/toolchain-floor-is-ghc-9-12-and-librdkafka-2-15.md) retains GHC 9.12 and librdkafka 2.15. No dependency upgrade is required.

[ADR-5](../adr/integration-tests-run-against-a-project-local-redpanda.md) requires the private broker at `127.0.0.1:39092`, random run-prefixed topics/groups, and cleanup limited to that run. Existing fixtures are in `test-integration/Kafka/TestEnv.hs`; register new modules in `test-integration/Main.hs` and the integration suite's `other-modules` in `rdkafka-hs.cabal`. Never use the shared port 9092. The baseline passed 14 unit and 116 integration tests, so add a regression that distinguishes this defect from the baseline.

Resolve dependency sources with Mori before changing bindings. The source project is `mori://confluentinc/librdkafka`; its project-relative `librdkafka/src/rdkafka.h` describes ownership, with implementation in the adjacent C files (source-file artifact URIs pending). Discover its location with the commands below and inspect only the scoped checkout, never `/nix/store` or filesystem root. Keep existing pins; any proposed bound change requires checking the authoritative registry and upstream release tags.


## Plan of Work


M1 adds a narrow internal helper that reads native partition errors while the list ForeignPtr is alive. Check the top-level error first; when it is success, inspect errors in input order and return the first substantive one. Do not make the generic `fromNativeTopicPartitionList` conversion throw, and do not reinterpret legitimate position/offset sentinel values as errors. Add the unassigned-partition regression before the fix. Add a mixed list with one assigned partition and one unknown/unassigned partition, proving an overall successful native call still yields a Haskell error when a member fails.

M2 documents and tests zero-timeout behavior against the supported native implementation. Preserve asynchronous initiation: normalize `RdKafkaRespErrInProgress` as pending only for `Timeout 0`; still report immediately populated substantive partition errors where librdkafka supplies them. For nonzero timeouts, do not silently normalize a residual error or pending state to completed success. Test empty input/top-level failure, all-success input, invalid offsets, and stable selection of the first error. Use positive tests with known partition offsets and payloads: consume, seek backward, and verify the next record is from the requested position in both callback modes. Exercise `rewindConsumer` after committing a known offset. State that mixed seeks can partially succeed and returning an error does not undo successful partitions. Add changelog and Haddocks.


M3 completes the seek/rewind boundary matrix. Exercise empty/single/multiple partition lists, duplicate entries, mixed successful/failed partitions, unknown topics, currently unassigned partitions, each supported logical offset, zero and nonzero timeouts, and positions before/after the retained log range. Compare exact next-record offsets and payloads after successful synchronous seeks. Rewind must distinguish no assignment, no committed offset, a known committed next offset, a committed-offset lookup failure, and a seek failure after lookup. In both callback modes, control reassignment/rebalance between lookup and seek with a barrier or a narrow test seam, then verify failure propagates without claiming rollback of already successful partitions. Derive native sentinels and timeout expectations from the supported header/source; do not invent equivalent treatment for every negative number. Unsupported input must either have a documented rejection test or a source-backed precondition row, not disappear from the matrix.

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

Use this focused group:

```bash
just test-it '--test-options=-p /seek-partitions/'
```

Reproduce the no-assignment case with `newAssignedConsumer`, a unique group ID, `noAutoCommit`, and `Timeout 1000`; destroy the consumer in cleanup. For positive seeks, use the private-broker `withTopic`/`produceTo` fixtures and serialize assignment, seek, and poll operations.


## Validation and Acceptance


An unassigned partition yields `Just (KafkaResponseError ...)`, matching its native error rather than `Nothing`. Mixed requests also return an error even when the assigned member succeeds. All-valid nonzero-timeout seeks return `Nothing` and subsequent polls prove the requested position. Empty requests preserve the native top-level error. Zero-timeout calls remain bounded and do not misreport an in-progress marker as a terminal failure, while immediate substantive errors remain visible. Rewind returns the seek result and successful rewind resumes at the committed offset. Both callback modes pass; no public type gains fields or constructors.


Each seek/rewind row in `docs/testing/coverage-matrix.json` names a result-code assertion and, where applicable, the subsequent consumer position. Include a mutation that ignores `err` fields and one that forgets the next-offset convention; the appropriate tests must fail. No success assertion may be only `isRight`, and no retry helper may discard unexpected errors while waiting for a record. Existing `seek` remains in EP-14's public API inventory even though deprecated; this child tests the supported replacement and rewind.

All new cases use EP-13's failure-preserving fixtures and are recorded with exact test names and candidate evidence in the coverage inventory. No known failing case may be marked expected-failure to claim completion.

## Idempotence and Recovery


These changes require no data migration. Re-running tests uses fresh run prefixes and must delete only resources created by that run. Use bounded waits and `bracket`/`finally` for fixture cleanup; a failed assertion must not leave a producer flushing indefinitely. Do not reset the shared broker or overwrite unrelated edits. If a proof fails, leave the milestone unfinished, record the failing behavior, and fix the implementation or fixture before broadening the test matrix. Do not publish or move release tags.


## Interfaces and Dependencies


Hard dependency: `docs/plans/13-make-coverage-evidence-and-test-fixtures-reliable.md` must be Complete; its trustworthy fixtures and coverage inventory are prerequisites. This plan owns `seekPartitions` and an internal error reader in `src/Kafka/Consumer/Convert.hs`. Keep native list ownership local and do not add a second destroy. `docs/plans/11-own-native-callbacks-through-client-teardown.md` owns client lifetime machinery; preserve that implementation when integrating. The parent combined gate is in `docs/masterplans/2-fix-inherited-release-review-defects.md`.
