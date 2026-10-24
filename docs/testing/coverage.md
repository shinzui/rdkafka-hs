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
