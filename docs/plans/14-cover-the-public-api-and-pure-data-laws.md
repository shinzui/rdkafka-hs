---
id: 14
slug: cover-the-public-api-and-pure-data-laws
title: "Cover the public API and pure-data laws"
kind: exec-plan
created_at: 2026-10-24T04:50:25Z
intention: "intention_01m5p37ej8e3h92gkf2wp9j6qa"
master_plan: "docs/masterplans/2-fix-inherited-release-review-defects.md"
provenance:
  created_by:
    model: "gpt-6-astra"
    harness: "codex-cli"
    at: 2026-10-24T04:50:25Z
  revisions:
    - model: "gpt-6.1-sol"
      harness: "codex-cli"
      at: 2026-10-24T08:48:23Z
      mode: "implement"
      note: "Integrate the shared property adapter and generated producer composition milestone before closing EP-9; retain all other public-contract work as open."
---

# Cover the public API and pure-data laws


This ExecPlan is a living document. Keep its evidence, milestones, dependencies, and decisions current.


## Purpose / Big Picture


Close the supported public-API gaps that remain beyond the six repair findings. Exact broker results, generated-data laws, and meaningful failure cases replace the assumption that an exercised function is a tested contract. Every remaining inventory row gets a real test or a source-backed not-applicable rationale.


## Progress


- [x] M1: Generated-data properties verify public composition, conversion, record, header, and version laws with replayable failures.
- [ ] M2: Consumer, metadata, diagnostics, and deprecated public entry points have exact positive/negative contract tests.
- [ ] M3: All public-contract rows owned by this child pass normal and instrumented runs and survive deliberate fault/mutation checks.

In Progress; M1 is Complete. The earlier producer-composition integration remains accepted. The remaining data laws add 31 generated pure properties and five fixed cases for consumer maps/log levels/poll modes, subscription topic/property composition, ordered binary headers, record functor/bifunctor/fold/traversal laws and deprecated adapters, scalar representations, enum mappings and full version-byte decoding. Two generated native properties observe consumer registration order and actual next-offset fields, including the signed 64-bit boundary; two broker cases verify subscription precedence in both callback modes.

Normal and focused instrumented runs pass all 65 unit and four native cases, with 1,000 cases per generated property and seed 20261024. Eight compiled semantic mutations fail, shrink and replay their full generator-state tokens; both controls pass. Nix/package/binding/documentation checks pass. [Public property evidence](../testing/public-properties-evidence.json) records exact scope and artifacts; [producer property evidence](../testing/producer-properties-evidence.json) retains the earlier milestone. The latest full normal checkpoint passes 65 unit and 495 integration cases before subsequent diagnostics/runtime/metadata additions; the full mapped checkpoint below passes 78 unit and 518 integration cases; inventory reconciliation remains partial. Public API state/error/diagnostic coverage, the saved deprecated-seek failure and the remaining M2/M3 requirements remain open. Scoped polling and rebalance checkpoints below close the two ownership discoveries without closing M2.

## Surprises & Discoveries

Fifteen deprecated-wrapper cases pass normally and with Haskell/native instrumentation. Producer delivery reports prove flush cleanup after normal return, returned errors and thrown handlers; both consumer modes close after the same outcomes. Native construction failures preserve Left and configuration exceptions retain their types without invoking handlers. Public constructor/field aliases work; misleading constructor-visibility Haddock is corrected. Four compiled cleanup/result mutations are caught and the unchanged control passes. See [wrapper evidence](../testing/deprecated-runner-evidence.json). M2/M3 and whole-candidate reconciliation remain open.

The source audit exposed two coverage-map classification defects in generated enums and spaced c2hs hooks. EP-13 maintenance fixes the mapper with byte-identical emitted Haskell, two old-mapper failure controls and 24 passing tooling controls; see [mapping evidence](../testing/generated-mapping-evidence.json). Previous test assertions remain accepted, while the next full mapped collection must refresh the handwritten/generated metrics and classification keys.

Eight consumer-commit cases pass normally and with Haskell/native instrumentation in both callback modes. A passthrough observer verifies the actual native async flag; callbacks and broker reads verify next, explicit and stored/all offsets. Empty/NoOffset, mixed failure/partial success and coordinator timeout cases pass. Four compiled flag/offset/callback/null-list mutations are caught and the control passes. Package checks pass; [commit evidence](../testing/consumer-commit-evidence.json) records the corrected absent-offset expectation and the scoped mutation rerun. Another 203 private outbound-union reader/unused-variant ticks have source-backed classifications; only public outbound writer paths remain measured. The inventory awaits a full-candidate refresh.

A metadata-field sensitivity checkpoint uses distinct native-owned broker/port/origin/leader/replica/ISR/error values, a Unicode host and an explicitly configured range protocol. Fourteen cases pass normally, Haskell-instrumented and native-instrumented; three compiled field-substitution mutations fail and the unchanged control passes. One generated metadata property runs 1,000 cases with independent broker-port values. Package checks pass. See [field evidence](../testing/metadata-field-evidence.json). An additional source audit classifies 211 ticks in private helpers with no src callers, without waiving shared utilities or reachable error paths. Inventory test status remains at the preceding full-candidate checkpoint pending the next complete run.

Metadata convenience helpers now preserve errors on individual partitions from caller-provided and native metadata. The baseline fails both new error cases; thirteen focused checks pass normally, with Haskell coverage and native instrumentation in an isolated checkout based on c857191, independent of the callback-polling experiment. Five compiled mutations are caught and the unchanged control passes; package/binding/documentation/Nix checks pass. See [partition metadata evidence](../testing/metadata-partition-evidence.json). The supplied-metadata gap is repaired; whole-candidate inventory reconciliation remains pending.

Manual sync callback polling is preserved following the user decision. The original regression loses offset zero; the first prototype also fails three seek/assignment/pause cases. A partition-aware copied FIFO now passes all 14 interaction cases across both modes and the complete 130-case public-contract group with native instrumentation. Full normal/mapped runs pass 78 unit and 560 integration cases (normal 0.95/281.86 seconds; mapped 4.33/261.42 seconds), plus four actual nonthreaded runtime checks serving real sync callbacks. Eight compiled buffer mutations and all four retained state mutations are caught, each unchanged control passes, and 22 tooling controls plus package/binding/documentation/Nix checks pass. See [buffer evidence](../testing/callback-buffer-evidence.json) and [ADR-25](../adr/manual-sync-callback-polling-preserves-records.md). The inventory now has 132 tested and 49 open rows. A source-backed partial Storable audit classifies 484 missing private-write/default-forwarding ticks; 898 other handwritten misses remain unclassified. M2/M3 and the historical deprecated-seek diagnosis remain open.

Real Unicode group/client fixtures expose two separate metadata defects in both callback modes: the ASCII query encoder returns an empty result for an existing group, and correcting only the query exposes changed group identity from the ASCII decoder. The binding now uses the existing native text encoder and metadata uses its matching decoder. Ten focused normal/native-instrumented checks pass, including distinct Unicode member strings and binary member fields. Three compiled query/group/member decoder mutations are caught; the unchanged control passes. The full mapped candidate passes 78 unit and 533 integration cases in 1.40/243.47 seconds, plus four actual nonthreaded runtime executions and 22 tooling controls. The inventory now has 125 tested and 56 open rows after 39 additional builder, configuration, assignment/close and producer queue contracts are reconciled. [Unicode metadata evidence](../testing/metadata-unicode-evidence.json) records the scope; M2/M3 remain open. [ADR-23](../adr/metadata-results-have-scoped-ownership-and-explicit-errors.md) records the text identity rule. Remaining provided-metadata partition error obligations are still open.

The shared configuration boundary reproduces silent truncation for twelve embedded-NUL keys/values across producer global/topic and consumer global/subscription settings. Both shared setters now reject those inputs with KafkaInvalidConfigurationValue before marshalling; ordinary Unicode, empty-value defaults and native unknown-key errors remain covered. Fifteen focused checks pass normally, with Haskell coverage and against native AddressSanitizer. [ADR-24](../adr/configuration-strings-reject-embedded-nul.md) records the representation rule. Two compiled shared key/value guard mutations are caught and the unchanged control passes; [configuration evidence](../testing/config-string-evidence.json) records the discarded redundant-path mutation and the accepted stable-candidate rerun. The subsequent full 611-case mapped candidate includes all fifteen configuration tests and reconciles CFG-C-string-boundaries; M2/M3 remain open.

The data-law checkpoint adds twelve generated properties and an independent native error-table check: 78 unit and 518 integration cases pass in the full mapped run with 1,000 generated cases and seed 20261024. Four actual nonthreaded runtime executions supplement the same registered runtime cases; only the identical Kafka.Consumer MIX module is merged across builds. Three compiled data mutations fail their intended assertions and two controls pass. The compiler-derived inventory now has 86 tested rows and 95 explicitly open after reconciling 57 representation, mapping, version and diagnostic contracts. Twenty-two tooling controls pass. [Data evidence](../testing/public-data-evidence.json) records the native descriptor sentinel fixture correction, compiler-generated deriving classification and runtime merge correction. M2/M3 remain open; configuration boundaries, metadata Unicode behavior, consumer obligations, the saved deprecated-seek failure and remaining handwritten coverage misses are next.

Metadata now passes ten focused checks normally, Haskell-instrumented and with native instrumentation; five compiled mutations are caught. Native return cancellation leaked metadata/group results before owner registration; a shared masked scope now retains the client, copies results and explicitly finalizes them. Tests verify exact topic/partition identity, replicas, watermarks, timestamp boundaries and mixed/missing errors, plus real group stable/empty lifecycle and controlled native group-state strings. CompletingRebalance maps to GroupAwaitingSync, and unsupported states return a descriptive KafkaError. See [metadata evidence](../testing/metadata-evidence.json) and [ADR-23](../adr/metadata-results-have-scoped-ownership-and-explicit-errors.md). A fresh full mapped run and inventory reconciliation are in progress; remaining input-boundary, deprecated-seek and other public-contract obligations remain open.

The actual nonthreaded executable reproduces silent async failure: construction succeeds but consumption times out after ten seconds; sync reads the same fixture. Async construction now returns a precise `KafkaBadSpecification` before client allocation when the runtime lacks bound threads. Four focused checks pass in real threaded and nonthreaded builds, plus a threaded instrumented build; removing the guard in a compiled nonthreaded mutation is caught. Sync subscriptions and the deprecated runner retain exact record delivery and close behavior. See [runtime evidence](../testing/runtime-evidence.json) and [ADR-22](../adr/async-consumers-require-a-threaded-runtime.md). Remaining metadata, deprecated-seek diagnosis and M2/M3 reconciliation remain open.

Diagnostics now pass nine normal, Haskell-instrumented and native-instrumented checks, with three compiled mutations caught. The inherited baseline fails four of seven checks: both print functions consume the caller handle, and acquisition cancellation retains global/topic dump arrays. An owned duplicate and checked native close preserve caller ownership and flush output; a bracket scopes dump arrays through strict-map conversion. Native counters verify 200 array destructions, a retained-array control, acquisition cancellation and descriptor closure after injected stream errors. See [diagnostic evidence](../testing/diagnostic-evidence.json) and [ADR-21](../adr/diagnostics-borrow-handles-and-scope-native-dumps.md). The preceding state checkpoint also passed the full normal suite: 65 unit and 495 integration cases in 222.19 seconds; its manifest identifies that earlier binary scope. Whole M2/M3 remains open.

A consumer-state checkpoint now passes 26 exact cases in each normal, Haskell-instrumented and native-instrumented run. Four compiled mutations detect omitted pause, suppressed member errors, suppressed store errors and lost global topic settings. Consumer construction now merges explicit subscription keys after global settings; pause/resume/store/position/committed results preserve overall then member errors. Tests distinguish native invalid-offset success from genuine member failure and retain partial effects in mixed requests. See [state evidence](../testing/consumer-state-evidence.json), [ADR-20](../adr/consumer-partition-operations-report-member-errors.md) and the consumer extension of [ADR-12](../adr/producer-settings-merge-explicit-topic-keys-after-global-keys.md). Repeated deprecated seek succeeds in both modes but does not close the saved intermittent state failure. M2/M3 remain open.

The message acquisition audit reproduces five ownership failures: a batch decoder exception retains one unprocessed message, cancellation at native batch return retains all three, mid-conversion cancellation retains one, and single polling loses a message on cancellation immediately after native return in both modes. Four controls pass. Extending the probe to native header errors reproduces five more failures: batch and plain/attributed polling replace header failures with empty headers. The 10/14 failing baseline is `/tmp/rdkafka-ep14-message-baseline-headers.log`; the native counter releases retained objects only after the assertions.

The scoped repair passes 24 focused cases, including callback-event polling cancellation, batch size zero/negative/overflow, normal/infinite timeouts and signed native failure propagation. Native message cleanup must cover the entire acquired batch, not just the currently converted member. [ADR-18](../adr/polled-messages-have-one-scoped-owner.md) records the ownership rule. [Message ownership evidence](../testing/message-ownership-evidence.json) accepts all 24 cases normally (16.53 seconds), Haskell-instrumented and against AddressSanitizer-instrumented native code (16.40 seconds), plus four compiled semantic mutations and an unchanged control. Required package/binding/documentation/Nix checks pass. This remains an M2 checkpoint; other public API and cooperative-rebalance/seek obligations are open.

The rebalance baseline reproduces all twelve controlled failure cases: cooperative errors leak, both protocols suppress failure, and after-events incorrectly report success. Native actions now check their results; cooperative error decoding has one bracketed owner, and the existing contained callback boundary reports failure. Fifteen focused checks pass normally, with Haskell instrumentation and against instrumented native code; four compiled mutations are caught. Two real cooperative consumers verify successful assignment, exact record delivery and revocation, one per callback mode. Twelve isolated children use the installed bridge, native partition lists and controlled native failures; they do not establish multi-consumer group handoff behavior. See [rebalance evidence](../testing/rebalance-error-evidence.json) and [ADR-19](../adr/rebalance-actions-check-and-release-native-errors.md). The combined public-contract group passes 39 cases. This is another M2 checkpoint; remaining consumer state, metadata, diagnostics, deprecated seek and M3 reconciliation are open.

EP-11's native reference audit found a batch ownership window: `rdKafkaConsumeBatchQueue` returns raw messages without finalizers, and `pollMessageBatch` traverses `fromMessagePtr`. That converter destroys its current message in `finally`, but interruption or conversion failure can leave later messages unprocessed and unowned. Reproduce with a counted native boundary and repair the batch's complete ownership scope under this child's polling/batch acceptance. Do not close this discovery through a success-only batch run.


The original unit suite covers four consumer-property examples, seven record mapping/traversal examples, and three version examples. There are no generated property suites. The metadata integration scenario often checks only Right/length, and deprecated seek is covered more thoroughly than its replacement. Current producer and consumer Type modules expose constructors, so the inventory must use actual exports rather than Haddock prose saying a constructor is hidden.


The local dependency corpus has unreleased adapter 0.12 metadata. The current Hackage preferred registry and upstream `quickcheck-0.11.1` tag identify release 0.11.1; the released source was inspected at commit `f838314360d1c8f20f2919b376a5d2672aedc893`. Hackage revision 7 permits QuickCheck below 2.20. The executed Cabal plan resolves adapter 0.11.1, QuickCheck 2.19.0.0 and Tasty 1.5.5; runtime pins and existing Tasty bounds are unchanged.

The global-bias mutation shrinks to `[Global "shared" "", Global "shared" "a"]`; the callback-order mutation reduces to two distinguishable callbacks. The full replay tokens and outputs are retained in the property manifest. Functions are observed through native delivery rather than compared for equality.

## Decision Log

2026-10-24: The generated subscription model exposed inherited earlier-value precedence. Asked whether to preserve it or match the other property types; the user explicitly chose “Make later subscription values win.” Change the subscription map union, retain topic-set union, and record the observable change in the changelog and ADR-17. The baseline property shrinks to `[Reset Earliest, Reset Latest]`; real async/sync consumers verify the final earliest policy reads existing data.


2026-10-24: Use test-only `^>=0.11.1` for [mori://UnkindPartition/tasty/packages/tasty-quickcheck](mori://UnkindPartition/tasty/packages/tasty-quickcheck), verified against the [preferred package registry](https://hackage-content.haskell.org/package/tasty-quickcheck/preferred.json), [current revised package description](https://hackage-content.haskell.org/package/tasty-quickcheck-0.11.1/tasty-quickcheck.cabal) and upstream release tag. Keep the shared adapter/producer milestone separate from the rest of M1 and the full child's acceptance.

2026-10-24: Test declared behavior with independent models and real state observations. Re-exports and deprecated functions remain obligations unless formally removed; generated repetitions of the same happy path do not close missing error branches. A newly found bug in an existing supported contract remains a repair obligation, not an expected-failure exemption.


## Outcomes & Retrospective


M1 is accepted with independent generated models, native observations and replayed semantic controls. The subscription precedence change is explicitly user-selected and recorded in [ADR-17](../adr/subscription-properties-use-later-values.md). A native offset observer checks the actual converted signed field and scoped error/metadata ownership; real broker cases separately verify transactional effects under EP-12. Remaining M2/M3 implementation and whole-child acceptance are open.


## Context and Orientation


Use EP-13's inventory to enumerate exact symbols; candidate source modules include `src/Kafka/Types.hs`, `src/Kafka/Consumer/Types.hs`, `src/Kafka/Consumer/ConsumerProperties.hs`, `src/Kafka/Consumer/Subscription.hs`, `src/Kafka/Producer/ProducerProperties.hs`, `src/Kafka/Metadata.hs`, `src/Kafka/Dump.hs`, and `src/Kafka/Version.hs`, plus remaining functions in `src/Kafka/Consumer.hs`. Add focused unit modules under `test/Kafka` and integration modules such as `PublicConsumerContractTest.hs`, `MetadataContractTest.hs`, and `DiagnosticContractTest.hs`. Register every module explicitly. EP-8–EP-12 own their repaired behavior rows; reference those tests instead of duplicating ownership.

This repository is `rdkafka-hs`, a Haskell wrapper around librdkafka. The reviewed baseline is `9c41b3d`; planning commit `7646e69` added the six-defect repair initiative. `rdkafka-hs.cabal` lists the public modules and two test suites: `rdkafka-hs-test` has 14 baseline tests and `rdkafka-hs-integration` has 116. `test/Main.hs` and `test-integration/Main.hs` register cases; `test-integration/Kafka/TestEnv.hs` owns broker fixtures. Generated bindings come from `src/Kafka/Internal/RdKafka.chs`, with hand-written conversion and lifetime code alongside them.

Read [ADR-10](../adr/release-coverage-requires-behavior-and-ownership-evidence.md): coverage means evidence for each supported behavior and critical ownership branch, not a passing count. [ADR-5](../adr/integration-tests-run-against-a-project-local-redpanda.md) protects the shared broker: use the private endpoint at `127.0.0.1:39092`, run-prefixed resources, and only run-owned fault targets. [ADR-1](../adr/consumer-queue-and-teardown-ownership.md) distinguishes graceful close from destruction and requires one native deallocator. [ADR-4](../adr/toolchain-floor-is-ghc-9-12-and-librdkafka-2-15.md) retains the existing compiler/library floor; [ADR-2](../adr/release-validation-before-publication.md) requires exact-candidate validation before publication. Existing first-release platform/provisioning limits in [ADR-7](../adr/first-release-scope-and-distribution.md) describe the earlier release; the new compatibility plan must document its additional test environments without rewriting that release history.

Use Mori to locate dependency sources before relying on APIs. `mori://confluentinc/librdkafka` owns the native API. `mori://haskell/cabal` owns project-relative `cabal/doc/cabal-project-description-file.rst`, whose coverage section documents `--enable-coverage`. `mori://UnkindPartition/tasty/packages/tasty-quickcheck` supplies a property-test adapter; its source is under project-relative `tasty/quickcheck/Test/Tasty/QuickCheck.hs` in `mori://UnkindPartition/tasty`. Source-file artifact URIs are pending. Consult the local source first; verify current releases through authoritative package registries/upstream tags before choosing new test dependency bounds. No runtime dependency upgrade is implied. Do not inspect `/nix/store` or filesystem root.


## Plan of Work


M1 adds generated tests through a Tasty property adapter, preferring `tasty-quickcheck` after verifying its released API and selecting compatible test-only bounds. A property checks a law over generated inputs; shrinking reduces a failing input to a small counterexample, and a replay seed recreates it. Cover monoid identity/associativity and documented right bias for consumer/producer properties and subscriptions, including conflicting keys, empty values, log levels, callback order, and callback poll mode. Observe callback behavior rather than trying to compare functions for equality. Model headers as an ordered list: list round-trip, concatenation, duplicates, empty/binary values, and preservation of order; do not invent nullable header support that the public type cannot represent. Generate record functor/bifunctor/traversal law cases, offset next-position conventions, timestamp/offset boundary representations, text/newtype conversions, supported compression/debug enum mappings, and the actual valid domain of version packing/decoding. Expected values come from a simple independent model or documented native constants, not the implementation being tested. Add fixed edge regressions alongside generated inputs, print replay seeds, and preserve minimized counterexamples. Keep pure tests broker-free so Nix sandbox builds run them.

M2 completes public consumer behavior in both callback modes where applicable: creation with valid/invalid properties, empty/regex subscriptions, assignment replacement and unassignment, pause/resume, position and committed offsets before/after store and commit, synchronous/asynchronous commit reports, disabled auto-store/auto-commit, polling empty/nonempty/error queues, attributed origins, batch boundaries, and fatal-state behavior. Assert exact records/offsets and distinguish expected timeout/EOF from unexpected errors. Batch size zero/negative and timeout sentinels must be checked against documented preconditions in isolated safety probes if needed; do not crash the main test process to test invalid FFI inputs. Check both local helper output and actual broker/consumer effects. Native per-partition errors in APIs besides seek must not be hidden by success-only tests. Multi-consumer assignment/rebalance and outage scenarios belong to EP-15, while this child owns ordinary API state transitions.

For metadata, assert topic/partition identifiers, leader/replica sets consistent with the fixture, low/high watermarks after known production, timestamp-to-offset lookup before/at/after record times, empty/multiple/mixed-invalid requests, missing topics/groups, and metadata timeouts. Test group-state decoding across reachable lifecycle states and unsupported native values through a private conversion fixture; do not merely reproduce the current partial mapping as an expected crash. Keep exact fixtures independent of unrelated topics/groups on the broker. For diagnostic APIs test configuration values, native dump contents via a temporary handle, and documented handle ownership/flush/close behavior; detect descriptor leaks or unexpected closure using owned handles. Check version consistency and compile/run the existing examples and deprecated run wrappers with bounded callbacks. Verify public API claims for threaded/nonthreaded runtimes; if the current code silently fails a claimed mode, record and repair the contract instead of silently removing that matrix row.

M3 reconciles all rows and the measured coverage misses. Strengthen weak inherited assertions instead of only adding cases. Add small deliberate mutations in disposable working copies: wrong offset increment, lost header ordering, reversed property bias, omitted pause, and suppressed error propagation. Focused tests must detect the changed behavior. Do not mutate the working tree's real release candidate or count a compile failure as behavioral proof. Any newly exposed violation of an existing public contract gets a minimized regression and a fix in its owning repair child or here for unassigned functions. Document API changes requiring separate decisions instead of treating them as routine coverage work. Update README/testing documentation to match demonstrated behavior.


## Concrete Steps


Run from `/Users/shinzui/Keikaku/bokuno/rdkafka-hs`, or the current checkout root:

```bash
nix develop
mori registry show confluentinc/librdkafka --full
mori registry show haskell/cabal --full
mori registry show UnkindPartition/tasty --full
cabal build all -f examples -f integration
cabal test rdkafka-hs-test --test-show-details=direct
just test-it
```

Run focused cases while developing and the full required gates after meaningful changes. Preserve unrelated `.seihou/manifest.json` edits. Source changes require normal and instrumented validation; documentation-only evidence updates do not justify repeating all expensive scenarios. Commit implementation boundaries using Conventional Commits plus this child's ExecPlan trailer, `MasterPlan: docs/masterplans/2-fix-inherited-release-review-defects.md`, and `Intention: intention_01m5p37ej8e3h92gkf2wp9j6qa`.

After selecting and wiring the property adapter, use its locally verified options to record and replay seeds; document the exact invocation in `docs/testing/coverage.md`. Register a stable group `public-contract` for remaining integration cases:

```bash
cabal test rdkafka-hs-test --test-show-details=direct
just test-it '--test-options=-p /public-contract/'
just coverage
python3 scripts/check-coverage.py --allow-open
```

Run `just coverage-check` as a diagnostic before handoff. It may still reject rows owned by incomplete repair/environment children, but no row owned by EP-14 may remain open. Record mutation commands/results from disposable checkouts with only the deliberate semantic mutation applied.


## Validation and Acceptance


All pure laws have fixed boundaries plus generated cases and reproducible failure seeds; no property is tautologically derived from the implementation. Every public export assigned to this child has an observable positive case and each applicable error/boundary case. Broker results assert exact content, counts, offsets, and errors. Hidden/unsupported paths are classified with source-backed rationale rather than skipped. Known defects remain open until fixed. Normal optimized and HPC builds pass, and the selected semantic mutations are caught by runtime assertions. Remaining open rows must be owned explicitly by other unfinished children and still block the master gate. Test-only dependency bounds are registry/tag verified and included correctly in source distributions.


## Idempotence and Recovery


All runs must be repeatable with fresh run IDs. Store generated reports under ignored build/artifact directories; commit the inventory schema, requirements, concise evidence manifest, and commands, not large generated HTML. Do not remove another run's topics or stop a broker not owned by the fixture. On failure retain the seed, operation trace, exact environment, and failure log; restore owned fault fixtures in `finally` before reporting failure. A retry that passes does not erase an earlier failure: diagnose it or keep the row open. Unavailable required environments remain blockers. No tagging, upload, remote publication, or downstream repository mutation is part of this plan.


## Interfaces and Dependencies


Hard dependency on all of `docs/plans/13-make-coverage-evidence-and-test-fixtures-reliable.md`, whose fixtures, inventory schema, and coverage commands must be accepted first. Soft dependencies on `docs/plans/8-apply-topic-creation-configuration-safely.md`, `docs/plans/9-honor-producer-settings-and-release-rejected-callbacks.md`, `docs/plans/10-report-per-partition-seek-failures.md`, `docs/plans/11-own-native-callbacks-through-client-teardown.md`, and `docs/plans/12-release-transaction-error-objects.md`: consume their test evidence but leave ownership of their defects there. This child can complete its own rows while other owners are still pending. EP-15 and the master gate require all repairs and this child Complete.
