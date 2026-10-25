---
id: 3
slug: complete-deferred-coverage-and-compatibility-validation
title: "Complete deferred coverage and compatibility validation"
kind: master-plan
created_at: 2026-10-24T23:17:05Z
intention: "intention_01m5p37ej8e3h92gkf2wp9j6qa"
provenance:
  created_by:
    model: "gpt-6.1-sol"
    harness: "codex-cli"
    at: 2026-10-24T23:17:05Z
  revisions:
    - model: "gpt-6.1-sol"
      harness: "codex-cli"
      at: 2026-10-24T23:51:40Z
      mode: "implement"
      note: "Start EP-16 against accepted primary candidate and reconcile deferred contract evidence"
---

# Complete deferred coverage and compatibility validation

This is the follow-up to [MasterPlan 2](2-fix-inherited-release-review-defects.md). On 2026-10-24 the user explicitly chose to finish that release against its demonstrated primary environment and defer exhaustive coverage and broader compatibility work. This plan preserves those requirements and the work already done. Creating this plan does not authorize its implementation or any publication.

## Vision & Scope

After this initiative, users can distinguish tested Kafka/client/toolchain combinations from merely permitted dependency versions, reproduce failure and recovery behavior on actual isolated brokers, and inspect a source-bound explanation for every untested handwritten code path. An executable qualification gate must reject missing, failed or stale required evidence. This is broader qualification after the primary-environment release, not a reason to discard that release's repairs or restart its tests.

The current implementation retains all six original repairs, additional correctness/ownership fixes, and 786 registered tests (78 unit and 708 integration). Its complete normal and mapped runs both pass. Four extra nonthreaded runtime checks are collected separately. Counts describe a starting inventory, not completion criteria. Preserve source fingerprints and verify later results in `docs/testing/coverage-evidence.json` and `dist-coverage/evidence.json` rather than treating this snapshot as final proof.

The scope has five deliverables: finish public-contract/source coverage and the historical seek investigation; execute Linux and newer released compiler/native tuples with effective memory diagnostics; exercise Apache Kafka outages, leader changes, group recovery and transactions; prove real authentication, TLS and authorization including credential replacement after reconnect; combine the evidence in a fail-closed package/compatibility gate. A tuple means one exact OS, compiler, native library and broker combination. Representative tuples cover the meaningful axes; an unlimited Cartesian product is not required.

No runtime API expansion, unrelated dependency upgrades, general deployment platform, CI rollout, infrastructure purchase, destructive shared-broker action, downstream repository edit or release publication is included. Actual supported versions are chosen during implementation using authoritative registries/tags, not guessed now. A discovered violation of an existing contract is repaired with a regression; a new capability or supported platform claim requires an explicit scope decision.

## Decomposition Strategy

The public-coverage audit has different acceptance from real broker failures and from native memory instrumentation. Keep those responsibilities separate so a platform setup issue cannot obscure completed source/behavior work. Authentication requires its own credentials/certificates and permission boundaries. A final integration child consumes all accepted evidence and demonstrates that the gate actually rejects incomplete qualification.

[ADR-27](../adr/release-validation-has-explicit-primary-and-expanded-scopes.md) records the user-selected release boundary. [ADR-10](../adr/release-coverage-requires-behavior-and-ownership-evidence.md) retains the comprehensive qualification standard here. [ADR-1](../adr/consumer-queue-and-teardown-ownership.md) and [ADR-15](../adr/native-clients-own-persistent-haskell-callback-bridges.md) govern native/client callback lifetimes. [ADR-5](../adr/integration-tests-run-against-a-project-local-redpanda.md) protects the shared broker. [ADR-4](../adr/toolchain-floor-is-ghc-9-12-and-librdkafka-2-15.md) records the current toolchain floor; broad bounds are not execution evidence. [ADR-2](../adr/release-validation-before-publication.md) requires exact-candidate validation before any separately authorized publication.

## Exec-Plan Registry

| # | Deliverable | Path | Hard dependencies | Soft/integration dependencies | Status |
|---|---|---|---|---|---|
| 16 | Finish public contracts, handwritten coverage and seek investigation | [EP-16](../plans/16-close-deferred-public-contract-and-source-coverage-gaps.md) | Accepted primary candidate from MasterPlan 2 | Integrates all later tuple results | In Progress |
| 17 | Execute Linux/newer tuples and effective native lifetime diagnostics | [EP-17](../plans/17-validate-linux-and-released-toolchain-native-lifetimes.md) | Accepted primary candidate from MasterPlan 2 | Defines tuple evidence consumed by EP-18/19/20 | Complete |
| 18 | Prove distributed broker/group/transaction recovery | [EP-18](../plans/18-verify-distributed-broker-failure-and-group-recovery.md) | Accepted primary candidate from MasterPlan 2 | Integrates tuple schema with EP-17 and secure fixture with EP-19 | Complete |
| 19 | Prove authentication, TLS, ACLs and credential reconnect | [EP-19](../plans/19-verify-authenticated-kafka-and-credential-recovery.md) | Accepted primary candidate from MasterPlan 2 | Can reuse EP-18 Apache fixture; integrates with EP-17 schema | Complete |
| 20 | Enforce complete source/tuple/package qualification | [EP-20](../plans/20-enforce-the-complete-compatibility-evidence-gate.md) | EP-16, EP-17, EP-18 and EP-19 Complete | Consumes all manifests | Not Started |

## Dependency Graph

Implement no child merely because its document exists. First accept the primary candidate under `docs/masterplans/2-fix-inherited-release-review-defects.md` and its narrowed EP-14/15, preserving its source fingerprint. EP-16 through EP-19 can then proceed independently with coordinated edits. Suggested execution is EP-16, EP-17's tuple schema and Linux smoke, EP-18's representative failover, EP-19's representative authenticated roundtrip, their remaining variants, and EP-20. This is sequencing advice, not a hidden whole-child dependency between EP-17, EP-18 and EP-19.

Early integration must prove one real path before expanding variants: EP-17 loads the selected native library and runs a counted callback-lifetime case; EP-18 produces/consumes through one leader change; EP-19 performs an authenticated produce/consume and a real denied operation. Agree the tuple/evidence fields through these producer/consumer checks before running a larger matrix. EP-20 starts only after all four producing children are Complete. Missing required environments block comprehensive qualification; they do not retroactively invalidate the separately scoped primary release.

## Integration Points

`docs/testing/coverage-matrix.json` remains the authoritative public-contract ledger. `scripts/collect-coverage.py`, `scripts/coverage-preprocess.py`, `scripts/coverage_common.py` and `scripts/check-coverage.py` preserve compiler exports, zero-count unlinked modules, handwritten/generated separation, identical MIX hashes and source fingerprints. EP-16 owns any schema/owner extension needed for new plan IDs; do not silently change old owners or mark an unimplemented row tested. The original strict checker stays strict. The primary release uses its explicitly documented `--allow-open` validation and lists the transferred obligations; EP-20 must use full strict closure.

EP-17 owns `docs/testing/compatibility-matrix.json`'s shared tuple fields: OS/architecture, actual compiler and Cabal versions, loaded native version/path or build identity, broker version/topology, test configuration, source fingerprint, commands, hashes and result status. EP-18 and EP-19 extend it with scenario evidence; EP-20 validates it. Credentials are never evidence fields. Record native instrumentation limitations separately from successful findings. Darwin AddressSanitizer runs with leak detection disabled do not prove leak freedom.

EP-18 owns run-owned fault orchestration and group/transaction scenarios; EP-19 owns secure broker configuration, ephemeral users/certificates/ACLs and their cleanup. Reuse `test-integration/Kafka/TestEnv.hs` resource prefixes, exact delivery assertions and contained cleanup. New broker fixtures use ports distinct from the existing private Redpanda at 39092/admin 39644 and from machine-global 9092. No broad topic deletion or global cluster reset is allowed. Do not introduce another general-purpose runner for each scenario: use Cabal/Tasty modules and extend `scripts/test-mutations.hs`; temporary Python edits/probes are fine, new permanent scripts need a concrete tooling gap.

EP-20 owns the final `just test-compatibility` interface and may implement it using existing Haskell/shell tooling. It must reject partial matrices and stale rows, build/test a clean `cabal sdist` extraction, and reconcile README's tested claims. It does not publish, tag, push or edit downstream projects.

### Preserve and reuse completed work

Keep all source, test, C fixtures, registration, mutations and documentation from MasterPlan 2. Useful accepted checkpoints include `4daa8a6` (queue payload/constructor boundaries), `2137727` (partial partition-list ownership) and `b740b72` (fresh inherited seek fixtures). Earlier repairs remain documented in EP-8 through EP-14; do not reimplement them.

| Existing asset | Evidence to retain | Follow-up use |
|---|---|---|
| Six original repairs and semantic mutations | EP-8 through EP-12 and their `docs/testing/*evidence.json` manifests | Regression baseline for every required tuple |
| Native callback/client/message/metadata/diagnostic/partition ownership barriers and counters | `callback-native-diagnostics.json`, `message-ownership-evidence.json`, `metadata-evidence.json`, `diagnostic-evidence.json`, `partition-list-evidence.json` | Reuse under effective Linux and Darwin diagnostics |
| Queue payload/descriptor and constructor cleanup checks | `final-boundaries-evidence.json`, `queue-notification-evidence.json` | Preserve every inherited case and corrected fresh scalar observer |
| Generated properties and one Haskell mutation runner | `public-properties-evidence.json`, `producer-properties-evidence.json`, `scripts/test-mutations.hs` | Keep seeds, shrunk replay tokens and intended assertion checks |
| Source classifications and coordinate repair | `native-storable-coverage-audit.json`, `private-binding-coverage-audit.json`, `private-haskell-coverage-audit.json`, `compiler-forwarding-coverage-audit.json`, `coverage-classification-remap.json` | Reuse proofs only when the exact source still matches |
| Historical seek failure, corrected active-fetcher fixtures and rejected hypothesis | `legacy-seek-evidence.json`; ignored `dist-coverage/failures/ep8-sync-seek-state.log` | Investigate the unknown trigger without deleting the old failure or weakening native State propagation |
| Examples/deprecated wrappers and nonthreaded runtime proof | `example-evidence.json`, `deprecated-runner-evidence.json`, `runtime-evidence.json` | Replay packaged examples/wrappers and actual runtime variants |

`docs/testing/deferred-validation.md` maps the remaining obligations to these children. At the earlier full checkpoint `d466235`, the matrix had 181 rows, 146 tested and 35 open, and 255 handwritten misses lacked classifications. Subsequent tests already address many of these open API rows. Reconcile current full evidence first; these historical numbers must not be interpreted as 35 entirely unimplemented features or a requirement to duplicate existing tests.

## Progress

EP-17, EP-18 and EP-19 are Complete on the captured functional candidate `2ec6cfbb3416926925e68ad8e96da1322be55ec292821543a8279040762cd98c`; EP-16 remains In Progress. Primary normal/mapped runs each pass 78 unit plus 753 integration cases, with four separate nonthreaded runtime checks. Source-bound external registration adds nine broker and seventeen security cases (857 aggregate behavior cases), without importing HPC ticks. The 181-row ledger has 179 tested and two open rows. Two handwritten producer exception-cleanup ticks remain unexplained; the historical seek cause remains required by explicit user decision. Native-only historical replay now reproduces State while seek reaches state none and the partition that delivered records remains active; object identity is under investigation. Linux/newer GHC/native execution, effective 279-case native diagnostics, all nine distributed cases and all seventeen security cases are accepted with owned fixture cleanup and documented diagnostic limitations. EP-20 cannot start until EP-16 is Complete. No comprehensive qualification or publication is claimed.

## Surprises & Discoveries

The earlier expansion combined a release repair with exhaustive public-path auditing and several unprovisioned environments. It exposed real additional defects, whose fixes and regressions remain retained. It also made release completion open-ended. Separating release qualification from broader qualification is an explicit user decision, not a claim that deferred work passed.

## Decision Log

2026-10-24: The user selected narrowing EP-14 and EP-15 to the demonstrated primary environment, requested a comprehensive follow-up MasterPlan and explicitly required preserving started work. Transfer the broader obligations here without removing tests, deleting failed evidence, relabeling open rows as passing, or extending this turn into implementation of the follow-up.

2026-10-24: Use five concern-based children and a final whole-child integration gate. Preserve representative actual Linux, Apache Kafka, newer released compiler/native, authentication/authorization, fault recovery and effective leak/invalid-access controls; no aggregate percentage substitutes for these results.

## Outcomes & Retrospective

A comprehensive, independently resumable plan now records the deferred work and its reusable assets. No expanded-environment execution or complete source-path closure is claimed by plan creation. Fill implementation outcomes only after running the children and promote durable decisions through the profiled ADR workflow.
