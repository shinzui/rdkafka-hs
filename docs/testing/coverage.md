# Coverage evidence

MasterPlan 2 requires behavioral evidence for every supported public contract and critical native ownership path. `coverage-matrix.json` records the obligations and responsible child plans. Its initial open rows are deliberate gaps, not passing tests. A compiler-derived export snapshot includes re-exports, types, exported constructors and record fields. Each `symbol` row covers all its `members`; cases must exercise the applicable constructors, selectors and laws before the row can close.

Run commands from the repository root inside `nix develop`:

```bash
just test-it
cabal test rdkafka-hs-test --test-show-details=direct
just coverage
cabal build lib:rdkafka-hs
python3 scripts/check-coverage.py --allow-open
just coverage-check
```

The development check accepts explicitly owned open rows. The final check rejects them. No test count or percentage closes a contract. To close one, record the exact fully qualified Tasty test names, the measured input `fingerprint`, and `status: tested` only after all of that row's behavior/inputs/expected results pass. Split a row when some cases have different owners or incomplete evidence. Environment rows stay open until actual execution under EP-15. A non-applicable row requires a specific source-backed precondition; inaccessible environments and inconvenient failure injection remain open.

`just coverage` collects both suites into `dist-coverage/`, clears only that build directory's previous TIX data, and forces instrumentation recompilation. It fails if a suite fails, its TIX data is missing, the candidate changes during the run, source mapping is absent, or the measured modules omit library source. The evidence manifest records the revision, source/test/build/tooling content digest (including untracked additions), compiler/native versions, exact command, broker endpoint, complete test names, aggregate pass count, and artifact checksums. Test logs retain failure seeds and diagnostics. Large reports stay ignored. Documentation-only inventory/evidence edits do not change the measured digest; scripts and build recipes do.

The report directory contains each suite's library report, the combined TIX and text/XML metrics, every tick's count/span/label, source-attributed misses, unexercised hand-written functions, and separate hand-written/generated tick metrics labelled with HPC's exact box kinds. HPC measures Haskell expressions, alternatives, conditions and declarations. Native allocation balance, races and callback safety require separate controls; HPC cannot establish them. Normal optimized execution remains mandatory.

GHC 9.12 excludes HPC spans redirected to a different source file by a `LINE` pragma. Ordinary c2hs output therefore reports zero ticks for `Kafka.Internal.RdKafka`, including its hand-written wrappers. The coverage-only GHC `-F` adapter replaces c2hs `LINE` directives with blank lines, preserves physical generated coordinates, and emits a sidecar mapping back to `.chs` lines. It also restores the original filename for ordinary `.hs` input so GHC does not attribute its spans to a temporary preprocessor file. Production compilation is unchanged. Mapping segments that originate in c2hs directives are classified as generated; ordinary wrapper segments remain hand-written. Zero measured hand-written wrapper ticks fail collection. Cabal's HTML uses generated `.hs` coordinates; `reports/misses.json` maps those coordinates back to `.chs` for review.

Every uncovered hand-written tick is visible with an owning row. Before the final gate, exercise it or add an exact `module:line:label` entry under `miss_classifications`, with `status: unreachable` or `inapplicable` and a concrete source-backed `rationale`. Critical error/ownership paths require execution and native diagnostics; they cannot be excused by a broad percentage or a successful neighboring case. Generated marshalling misses remain visible separately.

After changing source, tests, build inputs or coverage tooling, rerun both normal and instrumented checks. Missing or modified artifacts, stale fingerprints, removed test names and new exports fail validation. This infrastructure does not run the compatibility matrix; EP-15 supplies that evidence separately.


Topic-admin verification inspects the private container with `container exec rdkafka-hs-rp-0 rpk topic describe <run-prefixed-topic> -c`. Isolated external tuples supply `RDKAFKA_TEST_RPK`, configured for their own broker, and `RDKAFKA_TEST_UNKNOWN_TOPIC_CONFIG_POLICY=ignore` or `reject`. Primary Redpanda v26.2.1 accepts unknown topic properties; Apache Kafka's rejection policy must be tested under its own declared fixture. The test checks exact native setter arguments and effective known settings in either policy. The evidence manifest records these non-secret fixture inputs.

The integration executable's `cbits/admin_ownership.c` counts application-owned native requests, options, queue references and events, and provides controlled native failure/response stages. It dispatches ordinary calls to librdkafka and filters calls made internally by that library. A positive control reproduces the old allocation-then-error acquisition pattern and observes its retained request before releasing it. These counters prove wrapper ownership boundaries; Linux leak/invalid-access diagnostics remain required by EP-15.

Run `python3 scripts/test-topic-mutations.py` inside the development shell to build an isolated source archive, pass its unchanged control, and prove that removing configuration application or ownership/error checks fails the intended focused test after successful compilation. Its logs and source-bound evidence stay under ignored `dist-topic-mutations/`.

Producer verification uses `Kafka.Integration.ProducerRegressionTest` and the test executable's `cbits/producer_ownership.c`. The shim observes configuration on the actual native client passed to `produceva`, and its barriers pause immediately before controlled rejection or after real native acceptance. Cancellation tests observe the killer blocked in `throwTo`; a thread launch or delay does not define the ownership window. Ordinary calls still dispatch to librdkafka. Header names/values are borrowed Haskell buffers through the call, while native header objects are allocated and released inside librdkafka; EP-15's native diagnostics remain mandatory.

The native mock protocol fixture delays Produce responses and records received Produce requests, proving a message reached the broker before in-flight purge assertions. It runs on owned ephemeral loopback listeners and is destroyed with its callback-free native anchor client. It does not replace actual Redpanda codec/configuration round trips or EP-15's broker/authentication matrix.

Throwing delivery callbacks run in isolated child processes so foreign-boundary failures and uncaught-exception handlers cannot corrupt the parent suite. Coverage collection gives each child a unique TIX path, requires successful global and per-message probe coverage, and merges those ticks into the integration report. Normal runs retain optional child artifacts under ignored `dist-callback-probes/`.

Run `python3 scripts/test-producer-mutations.py` to compile an isolated archive, pass its complete producer control, and reject semantic mutations of settings, callback release/transfer, masking, exception handling, empty-byte representation, input validation and headers. A premature-free safety mutation may abort GHC; it counts only when a native marker proves enqueue succeeded before the isolated process aborted. Source-bound logs and archive checksums stay under ignored `dist-producer-mutations/`.

Generated properties use the released adapter [mori://UnkindPartition/tasty/packages/tasty-quickcheck](mori://UnkindPartition/tasty/packages/tasty-quickcheck), selected at `^>=0.11.1` after checking Hackage and its upstream release tag. Shared generators live in `test-support/Kafka/PropertySupport.hs`, which is included in candidate fingerprints. Pure producer laws use an independent last-write model; callback composition is observed through actual delivery reports. Both suites print replay tokens and retain the adapter's case-count and replay options.

Run a seeded thousand-case check with:

```bash
cabal test rdkafka-hs-test --test-show-details=direct \
  --test-options='--quickcheck-tests=1000 --quickcheck-replay=20261024'
cabal test rdkafka-hs-integration -f integration --test-show-details=direct \
  --test-options='-p /producer-properties/ --quickcheck-tests=1000 --quickcheck-replay=20261024'
```

For an exact failure replay, pass the entire printed `--quickcheck-replay` value as one argument to the binary from `cabal list-bin`. A token includes both generator state and size. `python3 scripts/test-property-mutations.py` builds a disposable archive, catches reversed global/topic/log-level/callback precedence, preserves minimized counterexamples, and reruns each printed token. Its archive, controls, failure and replay logs stay under ignored `dist-property-mutations/`. The property evidence manifest records resolved test dependency versions as well as source identity; replay uses the recorded toolchain and dependencies.
