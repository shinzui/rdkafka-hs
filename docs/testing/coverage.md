# Coverage evidence

The user-selected primary release scope is recorded in [ADR-27](../adr/release-validation-has-explicit-primary-and-expanded-scopes.md). MasterPlan 2 retains all implemented tests and requires current complete primary normal/mapped execution, repair controls and package replay. [MasterPlan 3](../masterplans/3-complete-deferred-coverage-and-compatibility-validation.md) owns exhaustive public/source-path closure and broader compatibility/security qualification. [The deferred ledger](deferred-validation.md) preserves every outstanding obligation and reusable asset. `coverage-matrix.json` records the obligations and responsible child plans. Its initial open rows are deliberate gaps, not passing tests. A compiler-derived export snapshot includes re-exports, types, exported constructors and record fields. Each `symbol` row covers all its `members`; cases must exercise the applicable constructors, selectors and laws before the row can close.

Run commands from the repository root inside `nix develop`:

```bash
just test-it
cabal test rdkafka-hs-test --test-show-details=direct
just coverage
cabal build lib:rdkafka-hs
python3 scripts/check-coverage.py --allow-open
just coverage-check
```

The explicit `--allow-open` check validates exports, registered tests, ownership, artifacts and source freshness while preserving deferred rows; it is also the user-approved primary release inventory check. The strict final comprehensive check rejects those rows and belongs to MasterPlan 3. No test count or percentage closes a contract. To close one, record the exact fully qualified Tasty test names, the measured input `fingerprint`, and `status: tested` only after all of that row's behavior/inputs/expected results pass. Split a row when some cases have different owners or incomplete evidence. Environment rows stay open until actual execution under EP-17, EP-18 and EP-19. A non-applicable row requires a specific source-backed precondition; inaccessible environments and inconvenient failure injection remain open.

`just coverage` collects both suites into `dist-coverage/`, clears only that build directory's previous TIX data, and forces instrumentation recompilation. It fails if a suite fails, its TIX data is missing, the candidate changes during the run, source mapping is absent, or the measured modules omit library source. The evidence manifest records the revision, source/test/build/tooling content digest (including untracked additions), compiler/native versions, exact command, broker endpoint, complete test names, aggregate pass count, and artifact checksums. Test logs retain failure seeds and diagnostics. Large reports stay ignored. Documentation-only inventory/evidence edits do not change the measured digest; scripts and build recipes do.

The report directory contains each suite's library report, the combined TIX and text/XML metrics, every tick's count/span/label, source-attributed misses, unexercised hand-written functions, and separate hand-written/generated tick metrics labelled with HPC's exact box kinds. HPC measures Haskell expressions, alternatives, conditions and declarations. Native allocation balance, races and callback safety require separate controls; HPC cannot establish them. Normal optimized execution remains mandatory.

GHC 9.12 excludes HPC spans redirected to a different source file by a `LINE` pragma. Ordinary c2hs output therefore reports zero ticks for `Kafka.Internal.RdKafka`, including its hand-written wrappers. The coverage-only GHC `-F` adapter replaces c2hs `LINE` directives with blank lines, preserves physical generated coordinates, and emits a sidecar mapping back to `.chs` lines. It also restores the original filename for ordinary `.hs` input so GHC does not attribute its spans to a temporary preprocessor file. Production compilation is unchanged. Mapping segments that originate in c2hs directives are classified as generated; ordinary wrapper segments remain hand-written. Zero measured hand-written wrapper ticks fail collection. Cabal's HTML uses generated `.hs` coordinates; `reports/misses.json` maps those coordinates back to `.chs` for review.

Every uncovered hand-written tick is visible with an owning row. Before the final gate, exercise it or add an exact `module:line:label` entry under `miss_classifications`, with `status: unreachable` or `inapplicable` and a concrete source-backed `rationale`. Critical error/ownership paths require execution and native diagnostics; they cannot be excused by a broad percentage or a successful neighboring case. Generated marshalling misses remain visible separately.

After changing source, tests, build inputs or coverage tooling, rerun both normal and instrumented checks. Missing or modified artifacts, stale fingerprints, removed test names and new exports fail validation. This infrastructure does not run the compatibility matrix; EP-17/18/19 produce that evidence and EP-20 integrates it. Historical EP-15 references in the retained manifests describe their original ownership; the ledger records each transferred `previous_owner`.


Topic-admin verification inspects the private container with `container exec rdkafka-hs-rp-0 rpk topic describe <run-prefixed-topic> -c`. Isolated external tuples supply `RDKAFKA_TEST_RPK`, configured for their own broker, and `RDKAFKA_TEST_UNKNOWN_TOPIC_CONFIG_POLICY=ignore` or `reject`. Primary Redpanda v26.2.1 accepts unknown topic properties; Apache Kafka's rejection policy must be tested under its own declared fixture. The test checks exact native setter arguments and effective known settings in either policy. The evidence manifest records these non-secret fixture inputs.

The integration executable's `cbits/admin_ownership.c` counts application-owned native requests, options, queue references and events, and provides controlled native failure/response stages. It dispatches ordinary calls to librdkafka and filters calls made internally by that library. A positive control reproduces the old allocation-then-error acquisition pattern and observes its retained request before releasing it. These counters prove wrapper ownership boundaries; Linux leak/invalid-access diagnostics remain required by EP-15.

Run `just test-mutations topic` inside the development shell to build an isolated source archive, pass its unchanged control, and prove that removing configuration application or ownership/error checks fails the intended focused test after successful compilation. Current logs and source-bound evidence stay under ignored `dist-mutations/`; earlier Python-runner archives retain their original paths.

Producer verification uses `Kafka.Integration.ProducerRegressionTest` and the test executable's `cbits/producer_ownership.c`. The shim observes configuration on the actual native client passed to `produceva`, and its barriers pause immediately before controlled rejection or after real native acceptance. Cancellation tests observe the killer blocked in `throwTo`; a thread launch or delay does not define the ownership window. Ordinary calls still dispatch to librdkafka. Header names/values are borrowed Haskell buffers through the call, while native header objects are allocated and released inside librdkafka; EP-15's native diagnostics remain mandatory.

The native mock protocol fixture delays Produce responses and records received Produce requests, proving a message reached the broker before in-flight purge assertions. It runs on owned ephemeral loopback listeners and is destroyed with its callback-free native anchor client. It does not replace actual Redpanda codec/configuration round trips or EP-15's broker/authentication matrix.

Throwing delivery callbacks run in isolated child processes so foreign-boundary failures and uncaught-exception handlers cannot corrupt the parent suite. Coverage collection gives each child a unique TIX path, requires successful global and per-message probe coverage, and merges those ticks into the integration report. Normal runs retain optional child artifacts under ignored `dist-callback-probes/`.

Run `just test-mutations producer` to compile an isolated archive, pass its complete producer control, and reject semantic mutations of settings, callback release/transfer, masking, exception handling, empty-byte representation, input validation and headers. A premature-free safety mutation may abort GHC; it counts only when a native marker proves enqueue succeeded before the isolated process aborted. Source-bound logs and archive checksums stay under ignored `dist-mutations/`.

Generated properties use the released adapter [mori://UnkindPartition/tasty/packages/tasty-quickcheck](mori://UnkindPartition/tasty/packages/tasty-quickcheck), selected at `^>=0.11.1` after checking Hackage and its upstream release tag. Shared generators live in `test-support/Kafka/PropertySupport.hs`, which is included in candidate fingerprints. Pure producer laws use an independent last-write model; callback composition is observed through actual delivery reports. Both suites print replay tokens and retain the adapter's case-count and replay options.

Run a seeded thousand-case check with:

```bash
cabal test rdkafka-hs-test --test-show-details=direct \
  --test-options='--quickcheck-tests=1000 --quickcheck-replay=20261024'
cabal test rdkafka-hs-integration -f integration --test-show-details=direct \
  --test-options='-p /producer-properties/ --quickcheck-tests=1000 --quickcheck-replay=20261024'
```

For an exact failure replay, pass the entire printed `--quickcheck-replay` value as one argument to the binary from `cabal list-bin`. A token includes both generator state and size. `just test-mutations property` builds a disposable archive, catches reversed global/topic/log-level/callback precedence, preserves minimized counterexamples, and reruns each printed token. Its archive, controls, failure and replay logs stay under ignored `dist-mutations/`. The property evidence manifest records resolved test dependency versions as well as source identity; replay uses the recorded toolchain and dependencies.

The single Haskell runner replaces four Python mutation scripts. [The parity record](mutation-runner-parity.json) verifies all 32 original mutations and five controls with identical selected test counts, plus all four minimized property replays. It also records five callback ownership mutations and two controls, available through `just test-mutations callback`. Ordinary tests remain in the Cabal suites.

Seek/rewind regressions run in the `seek-partitions` group in both callback modes. They check native member codes, exact payload/offset replay, partial success, timeout initiation versus completion, duplicate native entries, and committed next offsets. A test-only committed-lookup barrier allows an actual unassignment before rewind's seek. A run-owned prefix trim verifies behavior below the retained log range; external tuples provide their broker-configured `RDKAFKA_TEST_RPK` executable. `nix develop -c just test-mutations seek` compiles an isolated source archive and requires seven semantic mutations to fail their intended tests, plus an unchanged control. The native seam is linked only into the integration executable and ordinary calls forward to the real library.

Transaction ownership uses the `transaction-errors` group and `cbits/transaction_ownership.c`. Returned native errors are counted separately from background allocations. The explicit control repeats the inherited three-call decode-without-destroy sequence on one nontransactional producer and verifies three outstanding objects before fixture cleanup; the repaired loop balances all 300 calls. Native flag fixtures allocate native objects and call the library's diagnostic setters without copying its opaque layout. Real broker cases cover fencing, abort-required delivery rejection and source/output atomicity in both modes.

Run `just test-mutations transaction` to require a passing 51-case control and ten compiled mutations covering error disposal, flag classification, group metadata and the source next-offset convention. [Acceptance evidence](transaction-evidence.json) and [native diagnostics](transaction-native-diagnostics.json) record the candidate, actual native library and commands. Run normal tests after the coverage command's preliminary default-directory build has finished; two concurrent Cabal builds must use distinct build directories.

Remaining public data laws live in `Kafka.PublicPropertiesSpec` and `Kafka.Integration.ConsumerPropertyTest`. The consumer registration property uses real constructor log callbacks, and a native observer checks generated source next-offset fields across the representable signed 64-bit domain. Subscription maps now prefer later values by the user's explicit choice, while topic sets still accumulate; both consumer modes verify the resulting native reset policy. `just test-mutations public` catches eight generated semantic faults and replays their minimized tokens. [The focused M1 record](public-properties-evidence.json) distinguishes these property/HPC checks from the final full-candidate coverage gate.

Polled-message ownership tests are in `public-contract.messages`. The test-only native observer records actual returned message objects and pauses at native-return or decoder boundaries; cancellation is injected only after the boundary and a blocked exception sender are observed. A retained-last-object control verifies the counter. `just test-mutations messages` catches omitted batch cleanup, unmasked single acquisition and suppressed header/acquisition errors. [The checkpoint evidence](message-ownership-evidence.json) records normal, Haskell-instrumented and native-instrumented results; remaining EP-14 API work and final global coverage are separate.

### Actual runtime variants

EP-14 checks async construction, synchronous record delivery and deprecated
runner cleanup in actual threaded and nonthreaded executables:

```bash
nix develop -c cabal test rdkafka-hs-integration -f integration --test-show-details=direct --test-options='-p /runtime-contract/'
nix develop -c cabal test rdkafka-hs-integration -f integration --builddir=dist-nonthreaded --ghc-options='-single-threaded -with-rtsopts=-T' --test-show-details=direct --test-options='-p /runtime-contract/'
nix develop -c just test-mutations runtime
```

The nonthreaded command overrides the integration executable's usual threaded
link and default `-N` options. Its observed RTS way is `rts_v`. The mutation
runner applies the same flags to the runtime family and omits `-N2`; other
families retain their original threaded invocation. See
[runtime evidence](runtime-evidence.json). Focused instrumented results do not
replace final full-candidate mapped coverage.

The accepted primary checkpoint passes 786 registered cases normally and mapped, plus four actual nonthreaded cases and 24 tooling controls. [Primary evidence](primary-validation-evidence.json) records clean package replay and the bounded actual example. The remaining 35 historical open rows and 192 unclassified handwritten ticks are preserved in [the source-bound deferred snapshot](deferred-validation-snapshot.json); the strict checker continues to reject comprehensive incompleteness.


EP-16's [current checkpoint](deferred-public-contract-evidence.json) passes
808 registered cases normally and mapped, plus four actual nonthreaded cases
and 27 tooling controls. Reconciliation closes 19 historical contract rows;
16 remain open with concrete owners and missing behaviors. This does not alter
the accepted earlier primary candidate or close comprehensive qualification.

Every handwritten applicability classification now carries its exact `source`
and whole-file `source_sha256`. The checker rejects a missing or changed binding
even with `--allow-open`; new measured evidence cannot silently renew an old
source proof. Fallback miss ownership comes from the ledger, and transferred rows
retain their historical `previous_owner`.

`just test-mutations source-edges` passes seven unchanged controls and catches
eight compiled faults covering absent error attribution, logical offsets, native
subscription errors, partial topic acquisition, constructor cleanup, complete
batch order and live producer log level. The log-level assertion reads the actual
native client at enqueue. `dumpKafkaConf` reads the retained construction
configuration, so its `log_level` can differ from a subsequent native setter.

Expanded qualification uses explicit executable modes. With a released Apache
distribution, Java and OpenSSL available, run:

```bash
export RDKAFKA_TEST_KAFKA_HOME=/path/to/apache-kafka
cabal run rdkafka-hs-integration -f integration -- --broker-qualification
cabal run rdkafka-hs-integration -f integration -- --authentication-qualification
```

These modes create and destroy their own broker processes and ephemeral ports;
missing prerequisites fail. Ordinary primary tests still use their private
Redpanda. The shared fixture owns processes; the security extension owns
ephemeral CA/keys, identities, TLS settings and ACLs. No credentials belong in
logs or evidence. [Authentication evidence](authentication-evidence.json) records
its incremental behavioral checkpoint and remaining clean/interruption gates.

`--native-lifetime` wraps any selected executable mode with bounded Haskell GC
and native client-destruction drain. Child probes inherit that mode through
`RDKAFKA_TEST_NATIVE_LIFETIME`. A timeout fails; nothing is silently suppressed.
The transaction flag controls additionally need a diagnostic native build that
exports `rd_kafka_error_set_fatal`, `rd_kafka_error_set_retriable` and
`rd_kafka_error_set_txn_requires_abort`. Standard Linux symbol maps hide them.
The fixture checks their availability before allocating control clients and
fails explicitly if absent. [Linux evidence](linux-tuple-evidence.json) retains
the exact linker-map diff; only these three exports change, not native object
layout or function bodies. Normal public/native compatibility paths need no
private symbol.

`just test-mutations credentials` passes the seven focused setter controls and
rejects four compiled faults: NUL username/password truncation, unmasked native
error acquisition, and omitted error destruction. Pending cancellation is
observed at native acquisition and decoder boundaries, with one real native
allocation and one destruction. These ownership checks complement actual
authentication and replacement-only ACL observations after reconnect.
