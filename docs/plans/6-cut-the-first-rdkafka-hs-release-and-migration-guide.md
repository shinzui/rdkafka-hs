---
id: 6
slug: cut-the-first-rdkafka-hs-release-and-migration-guide
title: "Cut the first rdkafka-hs release and migration guide"
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
      note: "Changes requested: rename retains incompatible >=5.3 bounds and proof occurs after publishing an immutable release."
  revisions:
    - model: "gpt-6-astra"
      harness: "codex-cli"
      at: 2026-10-22T00:00:20Z
      mode: "update"
      note: "Validate exact candidate and downstream version bounds before publication; defer completion to remote proof."
    - model: "claude-opus-5-5"
      harness: "claude-code"
      at: 2026-10-22T01:27:25Z
      mode: "implement"
      note: "Implement EP-6 M1: release candidate, migration guide, combined check, downstream proof."
---

# Cut the first rdkafka-hs release and migration guide

This ExecPlan is a living document. The sections Progress, Surprises & Discoveries,
Decision Log, and Outcomes & Retrospective must be kept up to date as work proceeds.
If durable project context changes, update or create ADRs in docs/adr/ in the same change.


## Purpose / Big Picture

After this plan, a downstream package can stop depending on Hackage's abandoned
`hw-kafka-client` and depend on `rdkafka-hs` instead by pinning an immutable tag,
`v1.0.0.0`, and changing the dependency name and version range in every affected component. The tag exists on
`https://github.com/shinzui/rdkafka-hs`, the `CHANGELOG.md` has a `1.0.0.0` section that
tells a reader everything that differs from hw-kafka-client 5.3.0, the README has a
migration guide, every validation the earlier plans introduced passes on the tagged
commit, and the claim is proven rather than assumed: a scratch copy of
`mori://shinzui/kafka-effectful`, the first downstream consumer, builds against the tag.
The two improvement requests' final acceptance criteria (IR-1 AC-3 and IR-2 AC-5, both
"a tagged commit carries the change") are satisfied here.


## Progress

- [x] M1 (2026-10-22): `CHANGELOG.md` has a dated `1.0.0.0` section, `README.md` has the migration
      guide, and the full validation set, combined feature test and scratch downstream build
      pass on the exact release candidate before publication. Candidate
      `63031a21d070819fe89afbacb6ce76238816748f`: clean tree; `cabal build all -f
      examples --enable-tests` and the integration build exit 0 with 0 warnings; unit
      suite `All 14 tests passed`; `just test-it` `All 98 tests passed (84.80s)`
      including `observer / async` and `/ sync` (the combined EP-4/EP-5 case);
      `just bindings-check` fresh; `nix build` passes and its log shows the 14 unit
      tests; `nix fmt` leaves 0 changes; both bundles and `mori validate` pass; flake
      checks pass; `cabal check` reports no errors or warnings; the sdist builds and
      passes its unit tests in a fresh directory. Downstream: a local clone of
      `kafka-effectful` pinned to the candidate with all five bounds changed builds
      `rdkafka-hs-1.0.0.0` and `kafka-effectful-0.3.1.0`, and its broker-free suite
      reports `All 44 tests passed`. The first candidate, `9143f6d`, failed that
      downstream proof (see Surprises) and was never published.
- [ ] M2: Annotated tag `v1.0.0.0` pushed to `origin`; GitHub release created from the
      changelog section; the published tag resolves to the verified candidate commit.
- [ ] M3: A scratch clone of `kafka-effectful` switched to `rdkafka-hs` at `v1.0.0.0`
      builds, with the evidence recorded here; the MasterPlan's Outcomes & Retrospective
      and the ADR distillation pass are done.


## Surprises & Discoveries

Review (2026-10-21): the downstream Cabal file contains five constraints
`hw-kafka-client >=5.3 && <6`, so a name-only replacement excludes the fork's 1.0.0.0.
Candidate proof now updates bounds and runs before publication. IR completion waits
for remote tag verification. Mori registry refresh currently fails because its database
is absent and must not be recorded as successful.

Release candidate (2026-10-22):

- The first candidate's downstream proof failed at configure time:
  `Package 'libcurl', required by 'rdkafka', not found`. EP-3's
  `pkgconfig-depends: rdkafka >=2.15` makes Cabal run `pkg-config --cflags rdkafka`,
  which needs the `.pc` files of librdkafka's `Requires.private` libraries; the
  shared `nix-haskell-flake` shells only expose librdkafka's, and this repository had
  hidden that by adding them in `flake.module.nix`. The floor is now a build-time
  `#error` on `RD_KAFKA_VERSION` with `extra-libraries: rdkafka`, the workaround is
  gone, and ADR-4 and ADR-8 record why.
- `cabal check` warned about missing upper bounds inherited from upstream; the library
  now bounds `bifunctors`, `bytestring`, `containers`, `text`, `transformers` and
  `unix` to the major versions the GHC 9.12 toolchain resolves.
- The Mori registry is available; `mori register --local` succeeded during EP-1.


## Decision Log

- Decision: The downstream proof is a throwaway local clone, never a commit to the
  downstream repository.
  Rationale: Editing the downstream repositories is excluded from this initiative; the
  proof only needs to show the pin resolves and the rename is sufficient.
  Date: 2026-10-21

- Decision: Make `shinzui/rdkafka-hs` public before pushing the tag, and fix the
  `ConsumerProperties` poll-mode defect in 1.0.0.0.
  Rationale: Both were the user's choices at the release gate (2026-10-22). Public
  visibility lets the remote downstream proof and every dependent fetch the tag
  without credentials; the fix (`cpCallbackPollMode :: Maybe CallbackPollMode`,
  `effectiveCallbackPollMode`) lands before the candidate so its validation covers
  it.
  Date: 2026-10-22


## Outcomes & Retrospective

(To be filled during and after implementation.)


## Context and Orientation

By the time this plan starts, the five earlier child plans of
`docs/masterplans/1-fork-hw-kafka-client-as-rdkafka-hs.md` are Complete: the package
`rdkafka-hs` at the repository root builds on GHC 9.12.4 against librdkafka 2.15 (EP-1,
EP-3), its unit suite runs under `cabal test rdkafka-hs-test` and `nix build`, its
integration suite runs under `just test-it` against the project-local Redpanda (EP-2),
`docs/bindings.md` is kept current by `just bindings-check` (EP-3), and the two
improvement requests under `docs/improvement-requests/` have implementation evidence
but remain open until release acceptance (EP-4, EP-5). `CHANGELOG.md` has an `Unreleased` section that every plan
appended to; `rdkafka-hs.cabal` already says `version: 1.0.0.0`. `docs/adr/` holds the adopted review records and implementation decisions; resolve
actual allocated handles rather than assuming consecutive ADR numbers. The GitHub repository `shinzui/rdkafka-hs` was created private by seihou's
`git-init` module (`.seihou/manifest.json` records `git.githubVisibility = private`), and
`origin` points at it.

The downstream packages that depend on `hw-kafka-client` today, from `mori registry
dependents haskell-works/hw-kafka-client`, are `mori://shinzui/kafka-effectful`,
`mori://shinzui/hw-kafka-streamly`, `mori://shinzui/shibuya-kafka-adapter`,
`mori://shinzui/shikigami`, `mori://shinzui/keiro-runtime-kenshou` and `mori://shinzui/mori`,
with `mori://shinzui/shoko` waiting on IR-2. `kafka-effectful` (checkout
`/Users/shinzui/Keikaku/bokuno/kafka-effectful`) is the simplest and the model for the
others: its `cabal.project` already carries a `source-repository-package` stanza pinning
`https://github.com/shinzui/hw-kafka-client.git` at commit `6caed636...` with a long
comment explaining why, and its `kafka-effectful.cabal` lists `hw-kafka-client` in
`build-depends`. Because `rdkafka-hs` keeps the `Kafka.*` module namespace, migrating it
means replacing that stanza with one pointing at `https://github.com/shinzui/rdkafka-hs.git`
with `tag: v1.0.0.0` and replacing every dependency entry with `rdkafka-hs >=1.0 && <1.1`; no import
changes. The reviewed Cabal file has five `hw-kafka-client >=5.3 && <6` entries
across components. A name-only replacement cannot solve against version 1.0.0.0.

A `source-repository-package` pin over HTTPS to a private repository works only for a
user whose git credential helper is authenticated (the `gh` CLI configures one), so the
repository's visibility is a decision this plan surfaces but does not make: the MasterPlan
tracks it as a gate. `gh release create` works for private repositories.

Mori's registry is refreshed by re-registering the checkout; this author's documentation
repositories use `mori register --local` for that.

ADR context: [release validation before publication](../adr/release-validation-before-publication.md)
requires candidate proof before publishing, and
[consumer queue and teardown ownership](../adr/consumer-queue-and-teardown-ownership.md)
requires combined event/assignment/teardown acceptance. All adopted local ADRs are relevant as inputs to the distillation pass this
plan performs at the end; none constrains the release mechanics. The MasterPlan's Vision &
Scope lists deliberate exclusions (no Hackage publication, no API field renames, no admin
API expansion, no GitHub Actions, no Linux broker orchestration, no downstream edits) that
this plan records in a newly allocated ADR so the reasoning survives the plans.


## Plan of Work

**Milestone 1: the release candidate.** Turn `CHANGELOG.md`'s `Unreleased` section into
`## [1.0.0.0] - <date>` with a fresh empty `Unreleased` above it. Make sure the section
reads as a complete account for someone coming from hw-kafka-client 5.3.0: the rename and
retained namespace; the GHC and librdkafka floors; the test framework change; everything
carried from upstream main; the fatal-error observability fix; the queue event
notification (`setConsumerQueueIOEvent`); the observer consumer additions
(`newAssignedConsumer`, `pollMessageAttributed`, `ConsumerMessageError`,
`destroyConsumer`); the lifecycle bindings (`flushProducerTimeout`, `purgeProducer`,
`assignmentLost`, `consumerClosed`, `setSaslCredentials`); `Kafka.Version`; and the
removed things (the `it` flag renamed to `integration`, hspec gone, legacy Nix files
gone). Add a "Migrating from hw-kafka-client" section to `README.md` with the source pin and dependency edits
(including version bounds in every library, test, benchmark or executable component), a note that
imports are unchanged, the behavioural differences worth knowing (fatal errors now arrive
in-band in async mode; `flushProducer` now uses `rd_kafka_flush`), and the requirements.
Run the full validation set listed in Concrete Steps and record the results in Progress.
Add a combined integration case in both poll modes: manually assign an idle consumer,
register its queue event, produce and observe a wakeup and attributed record, disable
the event, destroy the consumer, then close the descriptors. Reassignment must preserve
notification delivery. This is EP-6's reconciliation of EP-4 and EP-5.

Perform ADR distillation before the candidate commit, then build a scratch downstream
checkout with a local source-repository-package location pinned to that exact commit.
Update all five dependency ranges and build with tests enabled; run its broker-free
tests. Record the candidate SHA and results. Any source change requires a new candidate
and rerunning affected checks; publication must wait until these pass.

**Milestone 2: the tag.** Create an annotated tag `v1.0.0.0` on the release commit with
the changelog section as its message, push it, and create a GitHub release from the same
text. Check the remote tag resolves to the validated candidate SHA. Do not mark either
request complete until Milestone 3 has fetched and built that published tag.

**Milestone 3: remote proof and completion evidence.** Clone `kafka-effectful` into the scratchpad
directory, edit its `cabal.project` stanza to `location:
https://github.com/shinzui/rdkafka-hs.git` and `tag: v1.0.0.0`, replace
`hw-kafka-client >=5.3 && <6` with `rdkafka-hs >=1.0 && <1.1`
in every affected `build-depends` entry, and build it inside its own
dev shell (`nix develop` in the clone; it provides librdkafka). Run its test-suite if it
does not need a broker, and record in Progress which it was. A local-path build is only M1 candidate evidence; it cannot replace this remote
proof. If remote retrieval fails, keep M3 incomplete and resolve the documented access
path without changing repository visibility implicitly. After successful remote proof,
mark IR-1 and IR-2 completed, name the tag and resolved SHA in each Resolution section,
advance timestamps and use `okf log add`. Validate both bundles and commit this
post-release evidence separately; it is not part of the tagged source. Re-register
with Mori when the registry is available, fill the MasterPlan retrospective and mark
EP-6 Complete only when all acceptance, including registry refresh, is satisfied.
Remove only the task-owned scratch directory after retaining the evidence.


## Concrete Steps

All commands run from `/Users/shinzui/Keikaku/bokuno/rdkafka-hs` inside the dev shell
unless stated.

Release-candidate validation (Milestone 1), all of which must pass:

```bash
git status --short                           # empty
cabal build all -f examples   # require success and inspect for warnings
cabal test rdkafka-hs-test
just test-it
just bindings-check
nix build
nix fmt && git status --short                # empty
okf validate docs/adr --strict --profile docs/adr/profile.dhall --profile-enforce --log-enforce
okf validate docs/improvement-requests --strict \
  --profile docs/improvement-requests/profile.dhall --profile-enforce --log-enforce
mori validate
cabal check
cabal sdist                                  # the tarball must build: see below
```

Prove the source distribution is complete (cabal2nix and downstream pins build from the
tree, but a missing `extra-source-files` entry would still bite a future Hackage upload):

```bash
rdkafka_sdist_dir=$(mktemp -d)
tar -xzf dist-newstyle/sdist/rdkafka-hs-1.0.0.0.tar.gz -C "$rdkafka_sdist_dir"
(cd "$rdkafka_sdist_dir/rdkafka-hs-1.0.0.0" && cabal build)
```

Commit and prove the candidate before publication (Milestone 1):

```bash
git add CHANGELOG.md README.md docs test-integration rdkafka-hs.cabal
git commit -m "chore(release): rdkafka-hs 1.0.0.0" \
  --trailer "MasterPlan: docs/masterplans/1-fork-hw-kafka-client-as-rdkafka-hs.md" \
  --trailer "ExecPlan: docs/plans/6-cut-the-first-rdkafka-hs-release-and-migration-guide.md" \
  --trailer "Intention: intention_01m5gc388wecfsq431s3pgg1dd"
git rev-parse HEAD  # record this exact candidate SHA
```

Complete the M1 downstream proof below before executing publication (Milestone 2):

```bash
git tag -a v1.0.0.0 -F <(awk '/^## \[1.0.0.0\]/{f=1} /^## \[/&&!/1.0.0.0/{if(f)exit} f' CHANGELOG.md)
git push origin master v1.0.0.0
gh release create v1.0.0.0 --title "rdkafka-hs 1.0.0.0" \
  --notes-file <(awk '/^## \[1.0.0.0\]/{f=1} /^## \[/&&!/1.0.0.0/{if(f)exit} f' CHANGELOG.md)
git ls-remote origin refs/tags/v1.0.0.0 'refs/tags/v1.0.0.0^{}'
```

Downstream proof runs twice in a task-owned temporary directory: M1 uses the local
repository and candidate SHA; M3 uses the HTTPS repository and published tag. Choose a
fresh directory with `mktemp -d` for each pass, and record its path before cloning. The
following shows the M3 edit; for M1 substitute the local repository path and candidate
SHA in `cabal.project`.

```bash
git clone --local /Users/shinzui/Keikaku/bokuno/kafka-effectful kafka-effectful-proof
cd kafka-effectful-proof
sed -i '' 's#https://github.com/shinzui/hw-kafka-client.git#https://github.com/shinzui/rdkafka-hs.git#; s#tag: 6caed636898a78e9f6e5a9c93eeb5562cbb2580a#tag: v1.0.0.0#' cabal.project
python3 - <<'PYEDIT'
from pathlib import Path
p = Path('kafka-effectful.cabal')
s = p.read_text()
old = 'hw-kafka-client >=5.3 && <6'
assert s.count(old) == 5, 'Inspect changed downstream dependency stanzas before proceeding'
p.write_text(s.replace(old, 'rdkafka-hs >=1.0 && <1.1'))
PYEDIT
grep -n "rdkafka-hs" cabal.project kafka-effectful.cabal
nix develop --command cabal build all --enable-tests
# Run the broker-free test components identified in the downstream Cabal file.
```

Expected: cabal reports `Cloning into ...rdkafka-hs...`, checks out `v1.0.0.0`, builds
`rdkafka-hs-1.0.0.0` and then `kafka-effectful`, with no module-not-found errors (the
`Kafka.*` imports resolve unchanged). Every commit carries:

```text
MasterPlan: docs/masterplans/1-fork-hw-kafka-client-as-rdkafka-hs.md
ExecPlan: docs/plans/6-cut-the-first-rdkafka-hs-release-and-migration-guide.md
Intention: intention_01m5gc388wecfsq431s3pgg1dd
```


## Validation and Acceptance

Accepted when `git tag -l v1.0.0.0` on a fresh clone of `origin` prints the tag, the
commit it points at passes every command in the release-candidate list, `gh release view
v1.0.0.0` shows the changelog text, `okf validate` accepts both bundles with IR-1 and IR-2
`completed` and their log entries naming the tag, and the `kafka-effectful` scratch build
transcript (recorded in Progress) shows `rdkafka-hs-1.0.0.0` fetched from the tag and the
downstream package compiled. The MasterPlan registry shows all six plans Complete and its
Outcomes & Retrospective is written. The prepublication downstream proof must name the
same SHA as the published tag; the final request completion records are explicitly
post-release documentation. Registry refresh must succeed or remain an outstanding gate.


## Idempotence and Recovery

The changelog and README edits are ordinary. A tag can be moved only before it is pushed;
once `v1.0.0.0` is pushed it is immutable, and a defect found afterwards is a `1.0.0.1`
release, never a re-tag (downstream pins would silently change meaning). `gh release
create` fails if the release exists; use `gh release edit` to amend notes. The scratch
clone lives in the scratchpad directory and is deleted at the end; nothing in it is ever
pushed. `mori register --local` is idempotent.


## Interfaces and Dependencies

Prerequisites: every other child plan of
`docs/masterplans/1-fork-hw-kafka-client-as-rdkafka-hs.md` is Complete, and the
MasterPlan's visibility gate has an answer (private with authenticated downstream users,
or public).

Tools: `gh` (authenticated), `cabal`, `nix`, `okf`, `mori`. Interfaces produced: the tag
`v1.0.0.0`, the migration guide in `README.md`, and the `1.0.0.0` changelog section;
consumed by every downstream package listed in Context and Orientation when they migrate,
which is outside this initiative.
