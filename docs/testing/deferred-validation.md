# Deferred validation and preserved work

On 2026-10-24 the user explicitly narrowed the immediate release to the demonstrated primary environment and requested a comprehensive follow-up without losing started work. [ADR-27](../adr/release-validation-has-explicit-primary-and-expanded-scopes.md) records the boundary. [MasterPlan 3](../masterplans/3-complete-deferred-coverage-and-compatibility-validation.md) and its five children own the work below. The follow-up has since been implemented and accepted; its original transfer scope remains documented here.

This transfer ledger is now historical: [MasterPlan 3](../masterplans/3-complete-deferred-coverage-and-compatibility-validation.md)
is Complete. The [comprehensive manifest](comprehensive-validation-evidence.json)
records the accepted exact candidate, 181 closed rows, zero unexplained handwritten
ticks, 858 behavior registrations, all required representative tuples, effective
native controls and an isolated source-package replay. `just test-compatibility`
passes without `--allow-open`. The original transfer snapshot and primary release
evidence below retain their historical identities; no failed or deferred result
has been relabeled as an earlier success.

## Final qualification checkpoint

The final source/tooling fingerprint is
`f8c90d1ecf86b6a03a3d9e9f943ed473e9bec482e7e2f1a7aa4199bd9af34f51`.
Normal/mapped and isolated package suites each pass 78 unit plus 754 integration
cases. Nine distributed and seventeen security cases extend behavior registration
to 858 without importing external HPC ticks. Four actual nonthreaded checks run
on both required platforms. The ledger has 181 tested rows and no open row or
unexplained handwritten tick; 401 exact source classifications remain.

All five children are Complete. The historical seek requirement is accepted
through the causal late-topic-finalizer reconstruction and the isolated 182-case
differential. The earlier partial checkpoint and original transfer tables below
remain historical records, not current blockers.

## Release boundary

The current release retains all fixes and tests, complete primary normal/mapped runs, generated seeds, native diagnostic controls and semantic mutations. Primary inventory validation uses the explicit `--allow-open` mode; strict `just coverage-check` still rejects comprehensive gaps. New code changes must not be excused as deferred if they leave a known failing regression. Broader execution and exhaustive closure are not claimed by broad bounds, source inspection or this deferral.

## Original deferred scope (now complete)

| Owner | Original obligation | Preserved work |
|---|---|---|
| [EP-16](../plans/16-close-deferred-public-contract-and-source-coverage-gaps.md) | Reconcile every remaining public contract, close reachable source/binding gaps, audit exact-span applicability, and investigate the historical inactive-fetcher transition | All 786 registered tests, four nonthreaded checks, compiler export/miss tooling, four partial source audits, coordinate remap and legacy-seek evidence |
| [EP-17](../plans/17-validate-linux-and-released-toolchain-native-lifetimes.md) | Actual Linux and newer released compiler/native tuples; effective leak/invalid-access proof for client/bridge/header/message/metadata/partition lifetimes; complementary Darwin allocation evidence | Existing native counters, barriers, weak references, sanitizer control and focused instrumented runs; current Darwin ASan leak detection is disabled |
| [EP-18](../plans/18-verify-distributed-broker-failure-and-group-recovery.md) | Real Apache multi-broker leader changes/outages/reconnect, commit/poll recovery, eager/cooperative handoffs, assignmentLost True, transactional recovery/fencing/read-committed effects and unknown-topic config reject policy | Current local fencing/cooperative/commit/transaction cases and controlled native errors; these do not already prove distributed failure |
| [EP-19](../plans/19-verify-authenticated-kafka-and-credential-recovery.md) | Actual SASL authentication, TLS validation, ACL allow/deny, and producer/consumer credential replacement on observed future authentication after reconnect | Current setter/error-object ownership cases; these plaintext checks are retained without being mislabeled authenticated proof |
| [EP-20](../plans/20-enforce-the-complete-compatibility-evidence-gate.md) | Strict full inventory/coverage and all-tuple gate, stale/missing evidence controls, complete clean package replay and accurate tested claims | Existing strict checker, source-distribution/example/wrapper mechanics and exact fingerprints; preserve the separate primary release record |

## Exact historical open-row handoff

The transfer snapshot retains the original status/owner and every behavior/input/expected/ownership field. The current `coverage-matrix.json` records accepted closures and preserves transferred `previous_owner` values. The table names the child that closed each original obligation; none was removed to make the primary release green.

| Original row | Follow-up owner |
|---|---|
| `API-Kafka-Consumer-assign` | EP-16 |
| `API-Kafka-Consumer-assignmentLost` | EP-18 |
| `API-Kafka-Consumer-commitAllOffsets` | EP-16 |
| `API-Kafka-Consumer-commitOffsetMessage` | EP-16 |
| `API-Kafka-Consumer-pollMessage` | EP-16 |
| `API-Kafka-Consumer-pollMessageAttributed` | EP-16 |
| `API-Kafka-Consumer-pollMessageBatch` | EP-16 |
| `API-Kafka-Consumer-position` | EP-16 |
| `API-Kafka-Consumer-setConsumerQueueIOEvent` | EP-16 |
| `API-Kafka-Consumer-setSaslCredentials` | EP-19 |
| `API-Kafka-Consumer-storeOffsetMessage` | EP-16 |
| `API-Kafka-Consumer-storeOffsets` | EP-16 |
| `API-Kafka-Consumer-subscription` | EP-16 |
| `API-Kafka-Metadata-allConsumerGroupsInfo` | EP-16 |
| `API-Kafka-Metadata-allTopicsMetadata` | EP-16 |
| `API-Kafka-Metadata-consumerGroupInfo` | EP-16 |
| `API-Kafka-Metadata-offsetsForTime` | EP-16 |
| `API-Kafka-Metadata-offsetsForTime-prime` | EP-16 |
| `API-Kafka-Metadata-partitionWatermarkOffsets` | EP-16 |
| `API-Kafka-Metadata-topicMetadata` | EP-16 |
| `API-Kafka-Metadata-topicOffsetsForTime` | EP-16 |
| `API-Kafka-Metadata-watermarkOffsets` | EP-16 |
| `API-Kafka-Metadata-watermarkOffsets-prime` | EP-16 |
| `API-Kafka-Producer-Types-ImmediateError` | EP-16 |
| `API-Kafka-Producer-setSaslCredentials` | EP-19 |
| `OWN-partition-list` | EP-16 |
| `ENV-primary` | EP-20 |
| `ENV-linux` | EP-17 |
| `ENV-kafka` | EP-18 |
| `ENV-newer` | EP-17 |
| `OWN-source-coverage` | EP-16 |
| `OWN-binding-coverage` | EP-16 |
| `ENV-topic-configuration-policy` | EP-18 |
| `ENV-producer-native-headers` | EP-17 |
| `API-Kafka-Consumer-seek` | EP-16 |

## Preservation and resumption

All source/test/C fixtures and Haskell mutation families remain registered. Recent retained commits are `4daa8a6` (queue payload/constructor boundaries), `2137727` (partial partition-list cleanup) and `b740b72` (fresh inherited seek fixtures). Do not reset them, delete existing cases, recreate the four replaced Python mutation runners, or overwrite historical rejected-run notes.

Keep `legacy-seek-evidence.json` and its hash of `dist-coverage/failures/ep8-sync-seek-state.log`. The log proves State after an older assignment observation but does not identify its native transition; the max-poll hypothesis did not reproduce State. Existing tests preserve exact native State and recovery, and all ten positive scenarios now observe real delivery immediately before seeking. EP-16 closed the investigation through preserved causal native-identity reconstruction and the isolated lifetime-repair differential; the untraced original event remains unavailable retrospectively. See [the seek evidence](legacy-seek-evidence.json).

Source audit reuse is conditional on the exact source. `coverage-classification-remap.json` preserved all 334 classifications and moved 33 coordinates only through identical source lines. A new change needs another source-coordinate review. `callback-native-diagnostics.json` retains a failing invalid-access control; `partition-list-evidence.json` retains the four failing pre-fix encoding cases and 19 passing scoped ownership cases. These are started/completed assets, not backlog to repeat blindly.

Ignored build directories and temporary logs can disappear. The checked-in manifests, exact source/mutation descriptions, case names, seeds, version identities and hashes survive; regenerate missing raw artifacts from the matching candidate. Never invent results for an absent log. Use Mori to discover dependency sources, verify released versions before selecting tuples, and keep all broker/security actions on run-owned resources. No publication is authorized by the follow-up plan.
