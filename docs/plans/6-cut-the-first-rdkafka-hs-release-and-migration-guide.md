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
---

# Cut the first rdkafka-hs release and migration guide

This ExecPlan is a living document. The sections Progress, Surprises & Discoveries,
Decision Log, and Outcomes & Retrospective must be kept up to date as work proceeds.
If durable project context changes, update or create ADRs in docs/adr/ in the same change.


## Purpose / Big Picture

After this plan, a downstream package can stop depending on Hackage's abandoned
`hw-kafka-client` and depend on `rdkafka-hs` instead by pinning an immutable tag,
`v1.0.0.0`, and changing one `build-depends` line. The tag exists on
`https://github.com/shinzui/rdkafka-hs`, the `CHANGELOG.md` has a `1.0.0.0` section that
tells a reader everything that differs from hw-kafka-client 5.3.0, the README has a
migration guide, every validation the earlier plans introduced passes on the tagged
commit, and the claim is proven rather than assumed: a scratch copy of
`mori://shinzui/kafka-effectful`, the first downstream consumer, builds against the tag.
The two improvement requests' final acceptance criteria (IR-1 AC-3 and IR-2 AC-5, both
"a tagged commit carries the change") are satisfied here.


## Progress

- [ ] M1: `CHANGELOG.md` has a dated `1.0.0.0` section, `README.md` has the migration
      guide, and the full validation set passes on the release candidate.
- [ ] M2: Annotated tag `v1.0.0.0` pushed to `origin`; GitHub release created from the
      changelog section; the improvement-request bundle's log records the tag.
- [ ] M3: A scratch clone of `kafka-effectful` switched to `rdkafka-hs` at `v1.0.0.0`
      builds, with the evidence recorded here; the MasterPlan's Outcomes & Retrospective
      and the ADR distillation pass are done.


## Surprises & Discoveries

(None yet.)


## Decision Log

- Decision: The downstream proof is a throwaway local clone, never a commit to the
  downstream repository.
  Rationale: Editing the downstream repositories is excluded from this initiative; the
  proof only needs to show the pin resolves and the rename is sufficient.
  Date: 2026-10-21


## Outcomes & Retrospective

(To be filled during and after implementation.)


## Context and Orientation

By the time this plan starts, the five earlier child plans of
`docs/masterplans/1-fork-hw-kafka-client-as-rdkafka-hs.md` are Complete: the package
`rdkafka-hs` at the repository root builds on GHC 9.12.4 against librdkafka 2.15 (EP-1,
EP-3), its unit suite runs under `cabal test rdkafka-hs-test` and `nix build`, its
integration suite runs under `just test-it` against the project-local Redpanda (EP-2),
`docs/bindings.md` is kept current by `just bindings-check` (EP-3), and the two
improvement requests under `docs/improvement-requests/` are `completed` except for their
tag criteria (EP-4, EP-5). `CHANGELOG.md` has an `Unreleased` section that every plan
appended to; `rdkafka-hs.cabal` already says `version: 1.0.0.0`. `docs/adr/` holds ADR-1
through ADR-5. The GitHub repository `shinzui/rdkafka-hs` was created private by seihou's
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
with `tag: v1.0.0.0` and replacing the dependency name; no import changes.

A `source-repository-package` pin over HTTPS to a private repository works only for a
user whose git credential helper is authenticated (the `gh` CLI configures one), so the
repository's visibility is a decision this plan surfaces but does not make: the MasterPlan
tracks it as a gate. `gh release create` works for private repositories.

Mori's registry is refreshed by re-registering the checkout; this author's documentation
repositories use `mori register --local` for that.

ADR context: all five local ADRs are relevant as inputs to the distillation pass this
plan performs at the end; none constrains the release mechanics. The MasterPlan's Vision &
Scope lists deliberate exclusions (no Hackage publication, no API field renames, no admin
API expansion, no GitHub Actions, no Linux broker orchestration, no downstream edits) that
this plan records as ADR-6 so the reasoning survives the plans.


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
gone). Add a "Migrating from hw-kafka-client" section to `README.md` with the two edits
(the `source-repository-package` stanza and the `build-depends` rename), a note that
imports are unchanged, the behavioural differences worth knowing (fatal errors now arrive
in-band in async mode; `flushProducer` now uses `rd_kafka_flush`), and the requirements.
Run the full validation set listed in Concrete Steps and record the results in Progress.

**Milestone 2: the tag.** Create an annotated tag `v1.0.0.0` on the release commit with
the changelog section as its message, push it, and create a GitHub release from the same
text. Add a dated entry to `docs/improvement-requests/log.md` saying IR-1 AC-3 and IR-2
AC-5 are met by `v1.0.0.0`, and update each request's "Resolution" section with the tag.
Re-register the project with Mori so the registry reflects the bundles and the release.

**Milestone 3: proof and distillation.** Clone `kafka-effectful` into the scratchpad
directory, edit its `cabal.project` stanza to `location:
https://github.com/shinzui/rdkafka-hs.git` and `tag: v1.0.0.0`, replace
`hw-kafka-client` with `rdkafka-hs` in its `build-depends`, and build it inside its own
dev shell (`nix develop` in the clone; it provides librdkafka). Run its test-suite if it
does not need a broker, and record in Progress which it was. If the HTTPS clone of a
private repository fails for cabal, record that in Surprises & Discoveries, repeat the
proof with `location: /Users/shinzui/Keikaku/bokuno/rdkafka-hs`, and raise the
visibility gate with the user. Delete the clone. Then perform the distillation pass for the
whole initiative: read the Decision Log, Surprises & Discoveries and Outcomes &
Retrospective of the MasterPlan and all six child plans; promote durable context into
`docs/adr/` (at minimum ADR-6, "Deliberate exclusions of the fork initiative", and
amendments to ADR-1 through ADR-5 where implementation changed the reasoning); fill the
MasterPlan's Outcomes & Retrospective; mark EP-6 Complete in the registry.


## Concrete Steps

All commands run from `/Users/shinzui/Keikaku/bokuno/rdkafka-hs` inside the dev shell
unless stated.

Release-candidate validation (Milestone 1), all of which must pass:

```bash
git status --short                           # empty
cabal build all -f examples 2>&1 | grep -c warning   # 0
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
tar -xzf dist-newstyle/sdist/rdkafka-hs-1.0.0.0.tar.gz -C "$TMPDIR"
(cd "$TMPDIR/rdkafka-hs-1.0.0.0" && cabal build)
```

Tag and release (Milestone 2):

```bash
git commit -am "chore(release): rdkafka-hs 1.0.0.0"
git tag -a v1.0.0.0 -F <(awk '/^## \[1.0.0.0\]/{f=1} /^## \[/&&!/1.0.0.0/{if(f)exit} f' CHANGELOG.md)
git push origin master v1.0.0.0
gh release create v1.0.0.0 --title "rdkafka-hs 1.0.0.0" \
  --notes-file <(awk '/^## \[1.0.0.0\]/{f=1} /^## \[/&&!/1.0.0.0/{if(f)exit} f' CHANGELOG.md)
mori register --local
```

Downstream proof (Milestone 3), from the scratchpad directory:

```bash
git clone --local /Users/shinzui/Keikaku/bokuno/kafka-effectful kafka-effectful-proof
cd kafka-effectful-proof
sed -i '' 's#https://github.com/shinzui/hw-kafka-client.git#https://github.com/shinzui/rdkafka-hs.git#; s#tag: 6caed636898a78e9f6e5a9c93eeb5562cbb2580a#tag: v1.0.0.0#' cabal.project
sed -i '' 's/hw-kafka-client/rdkafka-hs/' kafka-effectful.cabal
grep -n "rdkafka-hs" cabal.project kafka-effectful.cabal
nix develop --command cabal build all
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
Outcomes & Retrospective is written.


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
