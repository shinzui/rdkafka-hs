---
id: 1
slug: bootstrap-rdkafka-hs-from-the-hw-kafka-client-fork
title: "Bootstrap rdkafka-hs from the hw-kafka-client fork"
kind: exec-plan
created_at: 2026-10-21T23:17:55Z
intention: "intention_01m5gc388wecfsq431s3pgg1dd"
master_plan: "docs/masterplans/1-fork-hw-kafka-client-as-rdkafka-hs.md"
provenance:
  created_by:
    model: "claude-fable-5-1"
    harness: "claude-code"
    at: 2026-10-21T23:17:55Z
  reviews:
    - model: "gpt-6-astra"
      harness: "codex-cli"
      at: 2026-10-21T23:52:39Z
      verdict: "changes-requested"
      note: "Changes requested: fork checkout and IR bundle claims are stale; import exact commit and recover source requests before acceptance."
  revisions:
    - model: "gpt-6-astra"
      harness: "codex-cli"
      at: 2026-10-22T00:00:20Z
      mode: "update"
      note: "Pin the recovered source/request commit and correct bootstrap, ADR adoption and compiler claims."
---

# Bootstrap rdkafka-hs from the hw-kafka-client fork

This ExecPlan is a living document. The sections Progress, Surprises & Discoveries,
Decision Log, and Outcomes & Retrospective must be kept up to date as work proceeds.
If durable project context changes, update or create ADRs in docs/adr/ in the same change.


## Purpose / Big Picture

After this plan, this repository contains a Haskell package named `rdkafka-hs` that is the
maintained successor of `hw-kafka-client`, the Haskell binding to librdkafka (the C client
library for Apache Kafka). A developer can run `nix develop` (or let direnv load the
shell), then `cabal build all` and `cabal test rdkafka-hs-test`, and see the library, its
example programs and its unit tests build and pass on GHC 9.12.4 with librdkafka 2.15
supplied by Nix. `nix build` produces the same package from a clean sandbox. `nix fmt`
leaves the tree unchanged, the pre-commit hook passes, and `git log` shows the complete
upstream history behind every inherited file. The README explains what the fork is, the
CHANGELOG lists what it carries beyond Hackage's hw-kafka-client 5.3.0, and the two
improvement requests written against the fork live under `docs/improvement-requests/`
next to a freshly adopted `docs/adr/` bundle holding the first two architecture decisions.

Nothing in this plan talks to a Kafka broker. The integration tests inherited from
upstream are carried over as files but are ported and run by the next plan,
`docs/plans/2-run-integration-tests-on-a-project-local-redpanda.md`.


## Progress

- [ ] M1: Fork history imported, legacy scaffolding removed, package renamed to
      `rdkafka-hs`, and `cabal build all` succeeds inside the Nix dev shell with
      librdkafka provided by the `nix.kafka` option.
- [ ] M2: Package meets the project standards: `GHC2024` common stanza, warning set and
      default extensions from the `haskell-library` template, explicit deriving
      strategies, postpositive `qualified` imports, `nix fmt` clean, pre-commit passing,
      unit tests ported to tasty and passing under both `cabal test rdkafka-hs-test` and
      `nix build`, examples building with `-f examples`.
- [ ] M3: README, CHANGELOG, `docs/improvement-requests/` carried over,
      `docs/adr/` adopted with ADR-1 (fork identity) and ADR-2 (toolchain floor), both
      bundles registered in `mori.dhall`, and `okf validate` plus `mori validate`
      passing.


## Surprises & Discoveries

Review (2026-10-21): fetched the source branch and verified the committed IR
bundle at `fe5328a81941e92b93c3f47de14878346c9a5b43`; the source checkout remains
on `main`. The import is now commit-pinned and no longer depends on untracked files.
The current flake exposes only default and ghc9124 shells, so secondary-compiler
support must be proven before advertising it.


## Decision Log

- Decision: Import the fork's git history with a merge of unrelated histories instead of
  copying files.
  Rationale: Preserves upstream attribution and keeps `git blame` meaningful on
  inherited code. Recorded in the MasterPlan's Decision Log on 2026-10-21.
  Date: 2026-10-21

- Decision: Keep the MIT license and the original copyright notice, adding this fork's
  copyright line beneath it.
  Rationale: MIT requires the original notice to be retained in derivative works; the
  `haskell-library` template's BSD-3 default does not apply to a fork.
  Date: 2026-10-21

- Decision: Enable `DeriveAnyClass`, `DuplicateRecordFields`, `OverloadedLabels` and
  `OverloadedStrings` as the standard requires, but do not rename any public record
  field or add `generic-lens` to the library's dependencies.
  Rationale: The standard mandates the extensions; applying the record-pattern
  conventions to the public types would break seven dependents. See the MasterPlan.
  Date: 2026-10-21


## Outcomes & Retrospective

(To be filled during and after implementation.)


## Context and Orientation

This repository, `/Users/shinzui/Keikaku/bokuno/rdkafka-hs`, currently holds only a
seihou scaffold. Seihou is this author's project generator; it applied the module
`nix-haskell-flake` (version 0.34.0) which produced `flake.nix`, `flake.lock`,
`nix/haskell.nix`, `nix/treefmt.nix`, `nix/pre-commit.nix`, `.envrc`, `fourmolu.yaml`,
`process-compose.yaml`, `flake.module.nix.example` and
`process-compose.override.yaml.example`. All of those files are *managed*: seihou rewrites
them on every `seihou run nix-haskell-flake`, so they must never be edited by hand.
Project-specific Nix lives in the *unmanaged* file `flake.module.nix`, which does not exist
yet and which this plan creates. The variables seihou used are recorded in
`.seihou/manifest.json`; the ones that matter here are `project.name = rdkafka-hs`,
`ghc.version = ghc9124`, and `nix.kafka = false`, which this plan flips to `true` so the
dev shell gains librdkafka and exports `CPATH`, `LIBRARY_PATH` and `PKG_CONFIG_PATH` for
it.

`nix/haskell.nix` already builds a package called `rdkafka-hs` from the repository root
with `callCabal2nix`, so the cabal file must be named `rdkafka-hs.cabal` and live at the
root. `mori.dhall` already identifies the project as `shinzui/rdkafka-hs`, a Haskell
library forked from `haskell-works/hw-kafka-client`, with one package `rdkafka-hs` at path
`.`. There is no cabal file, no `src/`, two unprofiled review ADRs under `docs/adr/`, no `README.md` and no
`CHANGELOG.md` yet.

The source to import is the fork checkout at `/Users/shinzui/Keikaku/bokuno/hw-kafka-client`
(Mori project `mori://shinzui/hw-kafka-client`). The checkout is currently on `main`; do not import HEAD. Fetch branch
`fix/async-consumer-fatal-observability` and import the reviewed commit
`fe5328a81941e92b93c3f47de14878346c9a5b43`. Its parent, the implementation commit
`6caed636898a78e9f6e5a9c93eeb5562cbb2580a`, is upstream's `main`
(`56be4a6`, the last upstream push, 2025-10-03) plus one commit by this author,
"fix(consumer): surface consumer fatal errors and stop leaking polled messages", which
binds `rd_kafka_fatal_error`, reports a raised fatal in-band from `pollMessage` and
`pollMessageBatch`, adds `consumerFatalError`, and makes the background callback-poll loop
destroy the messages it consumes. Upstream `main` itself carries twelve commits that
Hackage's 5.3.0 release does not: admin-API topic creation and deletion
(`Kafka.Topic`, `Kafka.Topic.Types`), a partition-list leak fix in `storeOffsets` and
`storeOffsetMessage`, and skipping `rd_kafka_subscribe` when the topic set is empty. The reviewed import commit additionally tracks `docs/improvement-requests/1-expose-queue-event-notification-for-consumers.md`
and `docs/improvement-requests/2-support-a-bounded-assign-only-observer-consumer.md`,
together with that directory's `profile.dhall`, `index.md` and `log.md`, are an OKF
improvement-request bundle that the history import brings into this repository. Preserve its request IDs and
origins; update IR-2 AC-5 to name the successor release repository, logging that migration.

The inherited layout is: `hw-kafka-client.cabal` (cabal-version 2.2, `Haskell2010`, a
library of thirteen exposed and eight hidden modules under `src/Kafka/`, an example
executable under `example/` behind flag `examples`, an hspec unit suite under `tests/`,
and an hspec integration suite under `tests-it/` behind flag `it`). The C binding is
`src/Kafka/Internal/RdKafka.chs`, a c2hs file: c2hs is a preprocessor that turns
`{#fun ...#}` and `{#enum ...#}` declarations into Haskell FFI imports by reading the C
header `rdkafka.h`, so the package needs `build-tool-depends: c2hs:c2hs` and the header at
build time. The library links `extra-libraries: rdkafka`. Legacy files to delete after the
merge: `default.nix`, `shell.nix`, `nix/nixpkgs.nix`, `nix/overrides.nix`,
`nix/sources.json`, `nix/sources.nix` (a niv-based Nix setup superseded by the flake),
`docker-compose.yml`, `build/`, `scripts/build-librdkafka`, `scripts/build-docs.sh`,
`project.sh`, `.github/`, `.vscode/`, `.stylish-haskell.yaml`, and `Setup.hs`.

The standards to apply come from this author's pattern catalogue,
`mori://shinzui/haskell-jitsurei/docs/core-standards` (local file
`/Users/shinzui/Keikaku/bokuno/haskell-jitsurei/patterns/core/standards.md`): GHC 9.12 or
newer, `default-language: GHC2024`, a `common` stanza enabling `DeriveAnyClass`,
`DuplicateRecordFields`, `OverloadedLabels` and `OverloadedStrings`, postpositive
`qualified` (`import Data.Text qualified as Text`), and never a strategy-less `deriving`
clause (`-Wmissing-deriving-strategies` enforces it). The concrete cabal shape is the
`haskell-library` template in `mori://shinzui/seihou-modules` (project-relative
`modules/haskell/haskell-library/files/library.cabal.tpl`; artifact URI pending), readable at
`/Users/shinzui/Keikaku/bokuno/seihou-modules/modules/haskell/haskell-library/files/library.cabal.tpl`:
its `common common-options` stanza carries `-Wall -Wcompat -Widentities
-Wincomplete-uni-patterns -Wincomplete-record-updates -Wredundant-constraints
-fhide-source-paths -Wmissing-export-lists -Wpartial-fields
-Wmissing-deriving-strategies`, and its test-suite uses `tasty ^>=1.5` and
`tasty-hunit ^>=0.10`. The template's `CHANGELOG.md.tpl` (Keep a Changelog) and
`README.md.tpl` are the models for M3. Formatting is fourmolu with the repository's
`fourmolu.yaml`, run through `nix fmt` (treefmt also runs `cabal-gild` on the cabal file
and `nixpkgs-fmt` on Nix).

The toolchain is pinned by the flake: GHC 9.12.4 (with HLS) as the default shell. A 9.14.1 shell is not currently exposed here;
verify the base flake capability before wiring it in the unmanaged extension. The locked nixpkgs
provides librdkafka 2.15.0 and c2hs 0.28.8.

ADR context: the review added [consumer queue and teardown ownership](../adr/consumer-queue-and-teardown-ownership.md)
and [release validation before publication](../adr/release-validation-before-publication.md).
Adopt these existing records into the bundle; preserve their decisions and allocate
handles with `okf id next`. All ADR numbers below are anticipated labels, not reserved
handles. Resolve the final handles and paths in affected child plans after adoption. The cross-repository decision that explains why
the fork exists is `mori://shinzui/keiro/okf/adrs/concepts/ADR-11` ("Kafka consumer fatal
errors are surfaced in-band in both poll modes"), which states that the fix must live in
the binding behind a fork pin because no downstream layer can observe a signal destroyed
inside the binding. This plan additionally writes fork-identity and toolchain-floor ADRs; the shared workflow is in
`agents/skills/exec-plan/ADR.md` and the adoption blueprint is `adopt-architecture-decisions`
from `mori://shinzui/okf-profiles` (local checkout
`/Users/shinzui/Keikaku/bokuno/okf-profiles/blueprints/adopt-architecture-decisions/`).


## Plan of Work

**Milestone 1: a package that builds.** Reconfigure the seihou module with
`nix.kafka=true` so the dev shell contains librdkafka and its headers, and commit the
regenerated managed files. Merge the reviewed commit into the current branch with
`--allow-unrelated-histories`, inspecting every conflict and resolving `.gitignore` and `.envrc` in favour of the
seihou versions (append the useful upstream ignores such as `*.hi`, `*.o`, `.librdkafka/`
if they are not already covered). Delete the legacy files listed above. Rename
`hw-kafka-client.cabal` to `rdkafka-hs.cabal` with `git mv`, set `name: rdkafka-hs`,
`version: 1.0.0.0`, `cabal-version: 3.0`, this author as maintainer, `homepage` and
`bug-reports` to `https://github.com/shinzui/rdkafka-hs`, the `source-repository head`
location likewise, and initially `tested-with: GHC == 9.12.4`. Add 9.14.1 only after an actual
secondary compiler build and unit test pass; the current flake exposes only default
and ghc9124 shells. Wire a secondary shell in the unmanaged extension if needed. Rename the test-suites
to `rdkafka-hs-test` and `rdkafka-hs-integration`, rename the flag `it` to `integration`,
move `tests/` to `test/` and `tests-it/` to `test-integration/` with `git mv`, and
change every `build-depends: hw-kafka-client` in the executable and test stanzas to
`rdkafka-hs`. Create `flake.module.nix` from the example, adding
`haskellProject.extraDevPackages = [ pkgs.haskellPackages.c2hs pkgs.just ]` so c2hs and
the task runner are on the shell path. Add a `justfile` with `build`, `test`, `fmt` and
`check` recipes modelled on project `mori://shinzui/kafka-effectful`, path `justfile`
(artifact URI pending).
Acceptance: `cabal build all` inside `nix develop` succeeds.

**Milestone 2: a package that meets the standards.** Replace the per-stanza
`default-language: Haskell2010` and the repeated `ghc-options` with one `common
common-options` stanza copied from the template and imported by every stanza. Remove the
`if impl(ghc <8.0)` conditional and the `semigroups` dependency; raise `base` to
`>=4.21 && <5`. Expect and fix the warnings the new set produces: strategy-less
`deriving` clauses in `src/Kafka/Types.hs`, `src/Kafka/Consumer/Types.hs`,
`src/Kafka/Metadata.hs`, `src/Kafka/Producer/Types.hs`, `src/Kafka/Topic/Types.hs`,
`src/Kafka/Internal/Setup.hs` and `src/Kafka/Consumer/ConsumerProperties.hs` become
`deriving stock (...)` or `deriving newtype (...)`; modules without export lists gain them;
partial record fields, if any, are documented or restructured without renaming. `GHC2024`
turns on `MonoLocalBinds`, which may make a few local bindings in `src/Kafka/Consumer.hs`
and `src/Kafka/Consumer/Convert.hs` need type annotations; `OverloadedStrings` may make a
few `String` literals ambiguous where a function is polymorphic in `IsString`; fix each at
the use site. The c2hs module keeps compiling under the common stanza; if c2hs's generated
code trips a warning that cannot be fixed in the `.chs` source, add
`{-# OPTIONS_GHC -Wno-... #-}` to that file only, with a comment. Convert every prepositive
`import qualified X as Y` to `import X qualified as Y`. Port `test/Kafka/Consumer/ConsumerRecordMapSpec.hs`
and `test/Kafka/Consumer/ConsumerRecordTraverseSpec.hs` from hspec to tasty: each becomes
a module exporting a `TestTree` (`describe` becomes `testGroup`, `it` becomes `testCase`,
`shouldBe` becomes `@?=`), and `test/Spec.hs` becomes `test/Main.hs` calling
`defaultMain`. Drop `hspec`, `hspec-discover`, `either` and `monad-loops` from the unit
suite's dependencies. Run `nix fmt` and commit the formatted tree. Confirm the pre-commit
hook runs on commit. Acceptance: `cabal build all -f examples` is warning-free,
`cabal test rdkafka-hs-test` passes, `nix build` passes (it runs the unit suite in the
sandbox), `nix fmt` then `git status` shows no changes.

**Milestone 3: a project that explains itself.** Rewrite `README.md`: what rdkafka-hs is,
its relation to hw-kafka-client (lineage, why forked, that `Kafka.*` imports are
unchanged), the GHC 9.12+ and librdkafka 2.15+ requirements, how to develop (`nix develop`,
`just build`, `just test`), and a placeholder section for integration tests that EP-2
fills. Keep the consumer and producer usage examples from the upstream README, updated to
the current API. Create `CHANGELOG.md` in Keep a Changelog form with an `Unreleased`
section whose first entries are: package renamed from hw-kafka-client with the `Kafka.*`
namespace kept; GHC floor raised to 9.12 with `GHC2024`; test framework changed to tasty;
and, carried from upstream main beyond 5.3.0, the admin topic create/delete API, the
`storeOffsets` partition-list leak fix, the empty-subscription fix; and the fatal-error
observability fix (`consumerFatalError`, in-band `RdKafkaRespErrFatal`, no leaked polled
messages). Retain the imported `docs/improvement-requests/` bundle. Amend IR-2 AC-5
to point at `https://github.com/shinzui/rdkafka-hs`, retaining its original origin and
recording the successor migration with `okf log add`. Adopt the
ADR bundle by running the `adopt-architecture-decisions` blueprint (it creates
`docs/adr/profile.dhall`, `index.md`, `log.md` and registers the bundle); then write
`docs/adr/1-rdkafka-hs-is-a-maintained-fork-of-hw-kafka-client.md` (package name,
retained namespace, restarted version line, history imported, upstream tracked in
`mori.dhall`, and the exclusion of public record-field renames) and
`docs/adr/2-toolchain-floor-is-ghc-9-12-and-librdkafka-2-15.md` (GHC2024, the common
stanza, why no older GHC, librdkafka floor from the Nix pin). Register both bundles in
`mori.dhall` under `okfBundles`, following the shape used by
`mori://shinzui/shibuya-kafka-adapter` in its `mori.dhall`. Acceptance: `okf validate`
passes for both bundles with their profiles and `mori validate` passes.


## Concrete Steps

All commands run from `/Users/shinzui/Keikaku/bokuno/rdkafka-hs` unless stated.

Reconfigure the seihou module and verify the managed files changed as expected:

```bash
seihou run nix-haskell-flake --var nix.kafka=true --diff
seihou run nix-haskell-flake --var nix.kafka=true
git diff --stat
git add flake.nix flake.lock nix .envrc .seihou/manifest.json
git commit -m "chore(nix): enable librdkafka in the dev shell" \
  --trailer "MasterPlan: docs/masterplans/1-fork-hw-kafka-client-as-rdkafka-hs.md" \
  --trailer "ExecPlan: docs/plans/1-bootstrap-rdkafka-hs-from-the-hw-kafka-client-fork.md" \
  --trailer "Intention: intention_01m5gc388wecfsq431s3pgg1dd"
```

If `seihou run` prompts for a variable, answer with the value recorded for it in
`.seihou/manifest.json` under `variables`. The diff should add `pkgs.rdkafka` and
`pkgs.rdkafka.dev` to the shell and three `export` lines for `CPATH`, `LIBRARY_PATH` and
`PKG_CONFIG_PATH` to the shell hook in `nix/haskell.nix`. Then check the header is
visible:

```bash
nix develop --command sh -c 'pkg-config --modversion rdkafka'
```

Expected output: `2.15.0`.

Import the fork's history:

```bash
git status --short                         # require a clean working tree
git rev-parse HEAD                        # record the pre-import recovery commit
git fetch https://github.com/shinzui/hw-kafka-client.git fix/async-consumer-fatal-observability
git cat-file -t fe5328a81941e92b93c3f47de14878346c9a5b43  # commit
git merge --allow-unrelated-histories --no-commit fe5328a81941e92b93c3f47de14878346c9a5b43
git checkout --ours .gitignore .envrc
git add .gitignore .envrc
git rm -q -r default.nix shell.nix nix/nixpkgs.nix nix/overrides.nix nix/sources.json nix/sources.nix \
  docker-compose.yml build scripts project.sh .github .vscode .stylish-haskell.yaml Setup.hs
git commit -m "chore: import hw-kafka-client fork history" \
  --trailer "MasterPlan: docs/masterplans/1-fork-hw-kafka-client-as-rdkafka-hs.md" \
  --trailer "ExecPlan: docs/plans/1-bootstrap-rdkafka-hs-from-the-hw-kafka-client-fork.md" \
  --trailer "Intention: intention_01m5gc388wecfsq431s3pgg1dd"
git log --oneline -3
```

Verify the merge retains the recorded pre-import HEAD as its first parent and
`fe5328a81941e92b93c3f47de14878346c9a5b43` as its second parent. The fatal fix
`6caed636898a78e9f6e5a9c93eeb5562cbb2580a` must remain reachable. Inspect actual
conflicts before resolving them; stop if unexpected inherited files would overwrite plans.

Rename and restructure:

```bash
git mv hw-kafka-client.cabal rdkafka-hs.cabal
git mv tests test
git mv tests-it test-integration
cp flake.module.nix.example flake.module.nix
```

Edit `rdkafka-hs.cabal` and `flake.module.nix` as described in Milestone 1, then:

```bash
git add -A && git commit -m "feat!: rename package to rdkafka-hs" \
  --trailer "MasterPlan: docs/masterplans/1-fork-hw-kafka-client-as-rdkafka-hs.md" \
  --trailer "ExecPlan: docs/plans/1-bootstrap-rdkafka-hs-from-the-hw-kafka-client-fork.md" \
  --trailer "Intention: intention_01m5gc388wecfsq431s3pgg1dd"
nix develop --command cabal build all
```

The first build compiles c2hs from Hackage if the dev shell does not already provide it;
it should finish with `rdkafka-hs-1.0.0.0` built and no `rd_kafka_*` link errors.

Apply the standards (Milestone 2), then:

```bash
nix develop --command cabal build all -f examples   # require success and inspect for warnings
nix develop --command cabal test rdkafka-hs-test
nix build
nix fmt && git status --short                                               # expect empty
```

The unit suite's output should end with `All 4 tests passed` (the two inherited specs
hold two cases each; adjust the number to what was ported).

Carry the knowledge bundles (Milestone 3):

```bash
git ls-files docs/improvement-requests   # bundle already included in the merge
seihou agent run adopt-architecture-decisions
okf id list docs/adr --profile docs/adr/profile.dhall
okf id next docs/adr --profile docs/adr/profile.dhall ADR  # use the allocated handle
```

Write the two ADRs, then validate:

```bash
okf validate docs/adr --strict --profile docs/adr/profile.dhall --profile-enforce --log-enforce
okf validate docs/improvement-requests --strict --profile docs/improvement-requests/profile.dhall --profile-enforce --log-enforce
mori validate
```

Every commit in this plan carries the trailers:

```text
MasterPlan: docs/masterplans/1-fork-hw-kafka-client-as-rdkafka-hs.md
ExecPlan: docs/plans/1-bootstrap-rdkafka-hs-from-the-hw-kafka-client-fork.md
Intention: intention_01m5gc388wecfsq431s3pgg1dd
```


## Validation and Acceptance

The plan is accepted when all of the following hold on a clean checkout of `master`:

`nix develop --command cabal build all -f examples` compiles the library, the example
executable `kafka-client-example`, without warnings; the integration suite stays disabled until EP-2.
`nix develop --command cabal test rdkafka-hs-test` reports every test passed. `nix build`
succeeds, proving
the sandboxed build found c2hs and librdkafka through cabal2nix's mapping of
`build-tool-depends` and `extra-libraries`. `nix fmt` followed by `git status --short`
prints nothing. Run the configured hooks on a real working commit; do not create a throwaway commit. `git log --follow src/Kafka/Consumer.hs` shows upstream commits going
back to 2016. `okf validate` passes for both bundles and `mori validate` passes.
`grep -rn "hw-kafka-client" rdkafka-hs.cabal` finds only the lineage note in the
`description` field.

Beyond compilation, the carried fix is observable without a broker: in `cabal repl`,
`:t Kafka.Consumer.consumerFatalError` prints
`KafkaConsumer -> m (Maybe (KafkaError, Text))`, confirming the fork's commit, not Hackage
5.3.0, is what was imported.


## Idempotence and Recovery

The seihou reconfiguration is idempotent; running it again with the same variables
reports no changes. If it reports a conflict on a managed file, nobody should have edited
that file; inspect with `--diff` and accept the regenerated version.

The history merge is the one step that is awkward to repeat. If it goes wrong before the
merge commit, `git merge --abort` restores the tree. After committing, inspect the merge and use a reviewed corrective commit rather
than resetting to the old scaffold, which would discard newer plans and configuration.
Never force-push; the remote is private and the branch is `master`.

Deleting legacy files and renaming the cabal file are plain git operations and can be
redone. The request bundle is tracked in the import commit and needs no filesystem copy.
`seihou agent run adopt-architecture-decisions` refuses to run twice once
`docs/adr/profile.dhall` exists, which is the desired behaviour.


## Interfaces and Dependencies

Libraries whose choice matters: `base >=4.21 && <5`, `bytestring`, `containers`, `text`,
`transformers`, `unix`, `bifunctors` (all already used), `tasty ^>=1.5` and
`tasty-hunit ^>=0.10` for tests, and `c2hs` as a build tool. Native: librdkafka 2.15 from
the locked nixpkgs via `extra-libraries: rdkafka` (EP-3 will change this to
`pkgconfig-depends`).

Artifacts this plan defines that later plans consume: `rdkafka-hs.cabal` with its
`common common-options` stanza and the `integration` flag (EP-2 builds the test-suite
behind it); `flake.module.nix` (EP-2 adds a `shellHook` line); `justfile` (EP-2 adds
`test-it` and `redpanda-*` recipes, EP-3 adds `bindings-inventory` and `bindings-check`);
`CHANGELOG.md`'s `Unreleased` section (every later plan appends); `docs/adr/` and
`docs/improvement-requests/` registered in `mori.dhall`; and the unchanged public module
list under `src/Kafka/`.

This is the first child plan of `docs/masterplans/1-fork-hw-kafka-client-as-rdkafka-hs.md`
and has no prerequisites.
