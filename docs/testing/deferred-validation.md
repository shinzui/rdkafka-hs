# Deferred validation and preserved work

On 2026-10-24 the user explicitly narrowed the immediate release to the demonstrated primary environment and requested a comprehensive follow-up without losing started work. [ADR-27](../adr/release-validation-has-explicit-primary-and-expanded-scopes.md) records the boundary. [MasterPlan 3](../masterplans/3-complete-deferred-coverage-and-compatibility-validation.md) and its five children own the work below. Plan creation does not begin that implementation or claim it passed.

The frozen checkpoint `060dade4e8a9c764dabee54df3030fa6d857be3ea1404a2bc680a45ddadd5e4e` passes 78 unit and 708 integration cases normally and mapped, plus four actual nonthreaded runtime cases. The full report measures 2,750 handwritten native-wrapper ticks. Its remaining 192 unclassified handwritten ticks are preserved by exact keys/counts in [the snapshot](deferred-validation-snapshot.json), with all raw misses and artifact hashes in [coverage evidence](coverage-evidence.json). The inventory retains 146 tested and 35 open historical rows. Many open rows already have new passing cases and need reconciliation, not duplicate implementations.

## Current implementation checkpoint

The historical snapshot above remains unchanged. The current functional source
`2ec6cfbb3416926925e68ad8e96da1322be55ec292821543a8279040762cd98c`
passes 78 unit and 753 integration cases normally and mapped, plus four separate
nonthreaded cases. Nine actual distributed-broker and seventeen actual security
cases contribute source-validated behavior registrations, without contributing
HPC ticks: 857 aggregate registered cases. The inventory now has 179 tested and
two open rows; 401 exact source classifications leave two unexplained handwritten
producer exception-cleanup ticks. See [current public evidence](deferred-public-contract-evidence.json).

EP-17, EP-18 and EP-19 are Complete for the named source-bound scenarios in the
[compatibility matrix](compatibility-matrix.json). EP-16 still requires the
historical seek cause (explicitly retained by the user) and the two source ticks.
Native-only tracing reproduced State with seek in state none while the delivered
partition stays active; native object identity remains under investigation.
EP-20 remains Not Started because its whole-child EP-16 prerequisite is unmet.
The tables below preserve the original handoff, not the current row statuses.

## Release boundary

The current release retains all fixes and tests, complete primary normal/mapped runs, generated seeds, native diagnostic controls and semantic mutations. Primary inventory validation uses the explicit `--allow-open` mode; strict `just coverage-check` still rejects comprehensive gaps. New code changes must not be excused as deferred if they leave a known failing regression. Broader execution and exhaustive closure are not claimed by broad bounds, source inspection or this deferral.

## Deferred scope

| Owner | Work still required | Existing work to reuse |
|---|---|---|
| [EP-16](../plans/16-close-deferred-public-contract-and-source-coverage-gaps.md) | Reconcile every remaining public contract, close reachable source/binding gaps, audit exact-span applicability, and investigate the historical inactive-fetcher transition | All 786 registered tests, four nonthreaded checks, compiler export/miss tooling, four partial source audits, coordinate remap and legacy-seek evidence |
| [EP-17](../plans/17-validate-linux-and-released-toolchain-native-lifetimes.md) | Actual Linux and newer released compiler/native tuples; effective leak/invalid-access proof for client/bridge/header/message/metadata/partition lifetimes; complementary Darwin allocation evidence | Existing native counters, barriers, weak references, sanitizer control and focused instrumented runs; current Darwin ASan leak detection is disabled |
| [EP-18](../plans/18-verify-distributed-broker-failure-and-group-recovery.md) | Real Apache multi-broker leader changes/outages/reconnect, commit/poll recovery, eager/cooperative handoffs, assignmentLost True, transactional recovery/fencing/read-committed effects and unknown-topic config reject policy | Current local fencing/cooperative/commit/transaction cases and controlled native errors; these do not already prove distributed failure |
| [EP-19](../plans/19-verify-authenticated-kafka-and-credential-recovery.md) | Actual SASL authentication, TLS validation, ACL allow/deny, and producer/consumer credential replacement on observed future authentication after reconnect | Current setter/error-object ownership cases; these plaintext checks are retained without being mislabeled authenticated proof |
| [EP-20](../plans/20-enforce-the-complete-compatibility-evidence-gate.md) | Strict full inventory/coverage and all-tuple gate, stale/missing evidence controls, complete clean package replay and accurate tested claims | Existing strict checker, source-distribution/example/wrapper mechanics and exact fingerprints; preserve the separate primary release record |

## Exact historical open-row handoff

These rows retain their original status/owner in `coverage-matrix.json` for provenance. The follow-up owner below is the responsible child in MasterPlan 3. The snapshot includes every original behavior/input/expected/ownership field; none was removed to make the primary release green.

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

Keep `legacy-seek-evidence.json` and its hash of `dist-coverage/failures/ep8-sync-seek-state.log`. The log proves State after an older assignment observation but does not identify its native transition; the max-poll hypothesis did not reproduce State. Existing tests preserve exact native State and recovery, and all ten positive scenarios now observe real delivery immediately before seeking. Further forensics belongs to EP-16, not an invented release explanation.

Source audit reuse is conditional on the exact source. `coverage-classification-remap.json` preserved all 334 classifications and moved 33 coordinates only through identical source lines. A new change needs another source-coordinate review. `callback-native-diagnostics.json` retains a failing invalid-access control; `partition-list-evidence.json` retains the four failing pre-fix encoding cases and 19 passing scoped ownership cases. These are started/completed assets, not backlog to repeat blindly.

Ignored build directories and temporary logs can disappear. The checked-in manifests, exact source/mutation descriptions, case names, seeds, version identities and hashes survive; regenerate missing raw artifacts from the matching candidate. Never invent results for an absent log. Use Mori to discover dependency sources, verify released versions before selecting tuples, and keep all broker/security actions on run-owned resources. No publication is authorized by the follow-up plan.
