---
id: 1
slug: fork-hw-kafka-client-as-rdkafka-hs
title: "Fork hw-kafka-client as rdkafka-hs"
kind: master-plan
created_at: 2026-10-21T23:17:55Z
intention: "intention_01m5gc388wecfsq431s3pgg1dd"
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
      note: "Changes requested: clarify queue/lifecycle ownership, defer IR completion to release, and validate downstream before publication."
  revisions:
    - model: "gpt-6-astra"
      harness: "codex-cli"
      at: 2026-10-22T00:00:20Z
      mode: "update"
      note: "Clarify shared ownership and release gates; cascade source-verified review fixes to all six children."
---

# Fork hw-kafka-client as rdkafka-hs

This MasterPlan is a living document. The sections Progress, Surprises & Discoveries,
Decision Log, and Outcomes & Retrospective must be kept up to date as work proceeds.
If durable project context changes, update or create ADRs in docs/adr/ in the same change.


## Vision & Scope

`hw-kafka-client` is the Haskell binding to librdkafka, the C client for Apache Kafka. Its
upstream repository, `haskell-works/hw-kafka-client`, last received a push on 2025-10-03,
its newest Hackage release (5.3.0, from 2023) predates twelve commits on its main branch,
and a pull request from this author that fixes an unobservable consumer fatal error in the
default polling mode has had no response. Three of this author's own packages
(`kafka-effectful`, `hw-kafka-streamly`, `shibuya-kafka-adapter`) and several applications
already build against a commit-pinned fork of that unmerged work, and two further
improvement requests written against the fork are blocked on nobody being able to release
it.

When this initiative is complete, this repository is that maintained successor. It is a
Haskell package named `rdkafka-hs`, built from the fork's history, that keeps the existing
`Kafka.*` module namespace so downstream packages migrate by changing the source pin and the dependency name
and version bounds in each affected Cabal component. It compiles only on GHC 9.12 and newer, uses the `GHC2024` language edition and the
conventions catalogued in `mori://shinzui/haskell-jitsurei/docs/core-standards`, and is
built and formatted by the seihou-managed Nix flake already scaffolded here. Its unit tests
run under `cabal test` and `nix build`; its integration tests run against a private
Redpanda cluster on Apple Container that the dev shell can start with one command, so no
test ever touches the machine-wide broker. The librdkafka binding is audited against the
pinned librdkafka 2.15 header, with an inventory that says which C functions are bound, and
the three known defects are fixed and covered by tests: the consumer fatal-error blindness
and message leak from the fork's existing commit, the missing queue event notification
(improvement request IR-1), and the assign-only observer consumer (improvement request
IR-2). The first tagged release carries all of it and a migration guide, proven by building
one downstream consumer against the tag.

Explicitly excluded from this initiative: publishing to Hackage, renaming public record
fields or restructuring the public API around `generic-lens` labels, binding the admin API
beyond the topic create and delete calls that upstream already carries, continuous
integration on GitHub Actions, Linux broker orchestration for integration tests, and
editing the downstream repositories themselves. Each exclusion is deliberate and is
recorded as an ADR candidate in Integration Points so the reasoning survives.


## Decomposition Strategy

The initiative is split by functional concern into six child plans, ordered so that each
one produces something a person can run and observe before the next one starts.

The first concern is having a package at all: importing the fork's history into this
repository, renaming the package, and bringing the build, tests, formatting, and project
documentation onto the standards this author uses everywhere else. That is EP-1. It is
deliberately the only plan that touches the shape of the cabal file, the Nix extension
file, and the knowledge bundles, so later plans extend rather than rearrange.

The second concern is being able to prove anything about a Kafka client, which needs a
broker. EP-2 provisions a project-local Redpanda through the `nix.redpanda` option of the
`nix-haskell-flake` seihou module, ports the inherited integration suite to the project's
test framework, and adds the first new test: a regression test for the fatal-error fix the
fork was created for. That test is also the early producer/consumer check for the harness,
because it needs two consumers, static group membership, and both callback poll modes.

The third concern is the binding layer itself. EP-3 raises the librdkafka floor to the
version the Nix pin provides, exposes the linked library's version, produces a checked
inventory of bound versus unbound C functions, and binds the small set of client-lifecycle
functions that have no owner elsewhere. It owns the conventions of
`src/Kafka/Internal/RdKafka.chs`, which the two feature plans then follow.

The fourth and fifth concerns are the two improvement requests carried over from the fork.
EP-4 exposes librdkafka's queue event notification (IR-1) and EP-5 makes a bounded,
assign-only observer consumer possible (IR-2). They are independent features with separate
consumers downstream, so they are separate plans that can proceed in parallel once the
harness and the binding conventions exist. Each adds bindings in its own section of the
c2hs module and its own test module, and each records implementation evidence; EP-6 closes the requests after release acceptance.

The sixth concern is releasing. EP-6 finalizes the changelog, tags the first release, writes
the migration guide, and proves the release by building `kafka-effectful` against the tag
in a scratch checkout.

Alternatives considered. Folding EP-2 into EP-1 was rejected because a package that builds
and passes unit tests is a complete, verifiable outcome on its own, and the broker work has
distinct risks (Apple Container, port contracts) that should not block the rename. Folding
the two improvement requests into EP-3 was rejected because each has an external consumer
and its own acceptance criteria, and a single "bindings" plan would have done most of the
initiative's work. Splitting EP-1 further (history import versus standards) was rejected
because the standards work is mechanical once the package builds and neither half is useful
alone. Drafting a separate plan for the public API modernization (unprefixed record fields,
`#label` access) was rejected and recorded as an exclusion: it would break every downstream
consumer for no functional gain, and it can be its own initiative later.

ADR context. This review established [consumer queue and teardown ownership](../adr/consumer-queue-and-teardown-ownership.md)
and [release validation before publication](../adr/release-validation-before-publication.md).
EP-1 adopts these existing records when establishing the profiled bundle. Three cross-repository records shape the decomposition and are
cited by their canonical Mori handles:

- `mori://shinzui/keiro/okf/adrs/concepts/ADR-11` records, from the consumer's side, the
  contract that a raised librdkafka consumer fatal is reported in-band from every poll in
  both callback poll modes as `KafkaResponseError RdKafkaRespErrFatal`, with the cause
  recoverable from `consumerFatalError`, and that the fix must live in the binding behind a
  fork pin. EP-2's regression test and EP-5's deterministic destroy honour that contract.
- `mori://shinzui/keiro-runtime-kenshou/okf/adrs/concepts/ADR-17` decides that local Kafka
  verification uses a disposable, run-owned Redpanda and never mutates the machine-global
  broker at `127.0.0.1:9092`. EP-2 adopts the same rule for this library's tests.
- The `redpanda-container` repository's ADR on resource naming and host ports
  (project `mori://shinzui/redpanda-container`, path
  `docs/adr/2-redpanda-container-naming-and-port-contract.md`; that project is not yet in
  the local Mori registry, so its artifact-level URI is pending) is what the seihou
  template's `nix.redpanda` option implements: a project needing a private cluster chooses
  its own names and ports explicitly. EP-2 inherits the ports already recorded in
  `.seihou/manifest.json`.

The two improvement requests are `mori://shinzui/hw-kafka-client/okf/improvement-requests/concepts/IR-1`
and `mori://shinzui/hw-kafka-client/okf/improvement-requests/concepts/IR-2`; their bundle is committed in the fork branch at
`fe5328a81941e92b93c3f47de14878346c9a5b43`, fetched during this review. EP-1 imports
that exact commit, which contains the fatal fix `6caed636898a78e9f6e5a9c93eeb5562cbb2580a`
and the request documents. It updates the copied IR-2 release target to this successor
repository, preserving the original request identity and origin.


## Exec-Plan Registry

| # | Title | Path | Hard Deps | Soft Deps | Status |
|---|-------|------|-----------|-----------|--------|
| 1 | Bootstrap rdkafka-hs from the hw-kafka-client fork | docs/plans/1-bootstrap-rdkafka-hs-from-the-hw-kafka-client-fork.md | None | None | Not Started |
| 2 | Run integration tests on a project-local Redpanda | docs/plans/2-run-integration-tests-on-a-project-local-redpanda.md | EP-1 | None | Not Started |
| 3 | Refresh the librdkafka bindings for 2.15 and newer | docs/plans/3-refresh-the-librdkafka-bindings-for-2-15-and-newer.md | EP-1; EP-2 (M3 only) | None | Not Started |
| 4 | Expose queue event notification for consumers | docs/plans/4-expose-queue-event-notification-for-consumers.md | EP-2, EP-3 | None | Not Started |
| 5 | Support a bounded assign-only observer consumer | docs/plans/5-support-a-bounded-assign-only-observer-consumer.md | EP-2, EP-3 | None | Not Started |
| 6 | Cut the first rdkafka-hs release and migration guide | docs/plans/6-cut-the-first-rdkafka-hs-release-and-migration-guide.md | EP-1, EP-2, EP-3, EP-4, EP-5 | None | Not Started |

Status values: Not Started, In Progress, Complete, Cancelled.
Hard Deps and Soft Deps reference other rows by their # prefix (e.g., EP-1, EP-3).
A whole-child hard dependency requires that child to be Complete. If only a milestone's
accepted output is needed, name it explicitly (e.g., EP-1 M1) and explain it below.

The one milestone-scoped dependency is EP-3 on EP-2: EP-3's first two milestones (the
librdkafka floor and version API, and the binding inventory) need only a building package,
but its third milestone adds client-lifecycle bindings whose acceptance is an integration
test, so that milestone cannot begin until EP-2 is Complete.


## Dependency Graph

The plans form a chain with one parallel pair: EP-1, then EP-2, then EP-3, then EP-4 and
EP-5 side by side, then EP-6.

EP-2 needs EP-1 because its integration suite is a cabal test-suite of the package EP-1
creates, and because EP-1 owns the `rdkafka-hs.cabal` common stanza, the `flake.module.nix`
extension file, and the `justfile` that EP-2 extends. EP-3 needs EP-1 for the same reason,
and needs EP-2 before starting its third milestone, as the registry explains. The explicit execution order remains EP-1, EP-2, EP-3
M1–M3, EP-4/EP-5, then EP-6; milestone granularity does not authorize overlap here. EP-4 and EP-5
need EP-2 because every acceptance criterion they carry is a test against a real broker,
and they need EP-3 because EP-3 defines the conventions for adding bindings to
`src/Kafka/Internal/RdKafka.chs` and the inventory check that fails when a binding is added
without updating `docs/bindings.md`. EP-4 and EP-5 have an integration dependency, not a soft ordering preference.
EP-5 owns assignment routing and teardown; EP-4 owns event registration on the selected
queue. Their combined acceptance is an assigned consumer that receives queue events and
can be destroyed safely, owned by EP-6 M1. The contributor integrating second must
reconcile shared code, test registration, Cabal dependencies and changelog entries. EP-6
needs everything, because the release it cuts must carry all of it.

EP-4 and EP-5 may be implemented in parallel by two sessions. Nothing else should be
parallelized: EP-2 and EP-3 both edit the cabal file and the test tree, and the gain from
overlapping them is smaller than the merge cost.


## Integration Points

**The cabal package description, `rdkafka-hs.cabal`.** EP-1 defines it: the package name,
the `common` stanza carrying `GHC2024`, the warning set and the default extensions, the
`examples` flag, the library's exposed and hidden module lists, and the unit test-suite
`rdkafka-hs-test`. EP-1 also renames the inherited manual flag to `integration`, default off, and
creates the disabled `rdkafka-hs-integration` stanza; EP-2 ports that suite and its fixtures. EP-3 adds `pkgconfig-depends: rdkafka >=2.15` and the exposed
module `Kafka.Version`. EP-4 and EP-5 add test modules to the integration suite's
`other-modules`. Nobody after EP-1 changes the stanza structure or the warning set.

**The c2hs binding module, `src/Kafka/Internal/RdKafka.chs`.** EP-3 owns its conventions:
one commented section per librdkafka header region, `{#fun ...#}` for plain calls, hand
written wrappers for anything needing buffers or finalizers, and the inventory script
`scripts/binding-inventory.sh` that regenerates `docs/bindings.md` from a verified
release header outside `/nix/store`, matching the pkg-config version. EP-3 documents
`RDKAFKA_SOURCE_HEADER`; every later binding check uses that same verified input. EP-4 adds the queue
event bindings in its own section; EP-5 needs no new C binding. Any plan that touches the
module must regenerate the inventory, and `just bindings-check` is the check that fails
when it was forgotten.

**The integration test harness, `test-integration/`.** EP-2 defines `Kafka.TestEnv`
(broker discovery from the `RDKAFKA_TEST_BROKER` environment variable, defaulting to
`127.0.0.1:39092`; a random per-run prefix for every topic and group name; `withProducer`,
`withConsumer`, and topic-creation fixtures) and `test-integration/Main.hs`, which runs
groups sequentially. EP-3, EP-4 and EP-5 add their own test modules and register them in
`Main.hs`; they must use the per-run prefix and never hard-code `9092`.

**The public consumer surface, `src/Kafka/Consumer.hs`.** EP-4 adds
`setConsumerQueueIOEvent`; EP-5 adds `newAssignedConsumer`, `pollMessageAttributed`,
`ConsumerMessageError`, and `destroyConsumer`. EP-3 owns the baseline module and its lifecycle additions; EP-5 owns manual-assignment
routing, shared forwarding helpers in `Kafka.Consumer.Callbacks`, and lifetime state in
`Kafka.Internal.Setup`. EP-4 follows that queue ownership contract and owns event
registration. They also share test registration, Cabal dependencies and CHANGELOG entries;
EP-6 M1 runs the combined assigned-consumer event and teardown check. Both also rely on the fatal-error check that
`pollMessage` already performs, and must not weaken it.

**The seihou-managed Nix files.** `flake.nix`, `flake.lock`, `nix/haskell.nix`,
`nix/treefmt.nix`, `nix/pre-commit.nix`, `nix/redpanda.nix`, `.envrc`, `fourmolu.yaml` and
`process-compose.yaml` are regenerated by `seihou run nix-haskell-flake` and must never be
edited by hand. EP-1 reconfigures the module with `nix.kafka=true`; EP-2 reconfigures it
with `nix.redpanda=true`. Everything project-specific lives in the unmanaged
`flake.module.nix` (EP-1 creates it; EP-2 adds the broker environment variable to its
`shellHook`) and `process-compose.override.yaml` (EP-2 creates it).

**The private broker's port contract.** The ports `39092` (Kafka), `39644` (Admin API),
`38081` (Schema Registry), `38082` (HTTP proxy) and `38080` (Console) are recorded in
`.seihou/manifest.json` under `redpanda.*-port`, rendered into `nix/redpanda.nix`, mirrored
by EP-2 into the `RDKAFKA_TEST_BROKER` default in `flake.module.nix` and
`test-integration/Kafka/TestEnv.hs`, and documented in `README.md`. Changing a port means
changing all four. EP-2 owns this contract.

**The knowledge bundles under `docs/`.** EP-1 carries `docs/improvement-requests/` over
from the fork and adopts `docs/adr/` through the `adopt-architecture-decisions` blueprint,
registering both in `mori.dhall`. EP-4 and EP-5 record implementation evidence while keeping the requests open.
EP-6 verifies both bundles before tagging, then marks the requests `completed` only
after remote tag acceptance. Preserve canonical origins and use `okf log add` when
updating their timestamps. EP-1 adopts the two existing review ADRs before allocating
new handles; numeric ADR handles in the original plans are provisional until allocated.

**`CHANGELOG.md`.** EP-1 creates it with an `Unreleased` section listing what the fork
carries beyond Hackage 5.3.0. Every later plan appends to `Unreleased`. EP-6 turns it into
the `1.0.0.0` entry.

Cross-plan decisions that should become ADRs, with the plan that writes each: the fork's
identity, package name, retained `Kafka.*` namespace and restarted version line (EP-1);
the toolchain floor of GHC 9.12 with `GHC2024` and librdkafka 2.15 (EP-1, amended by
EP-3); integration tests run only against a project-local Redpanda behind a manual cabal
flag (EP-2); consumer fatal errors are surfaced in-band and the message queue loop destroys
what it polls, the library-side mirror of keiro's ADR-11 (EP-2); consumer queue routing and teardown follow
[the ownership ADR](../adr/consumer-queue-and-teardown-ownership.md) (EP-5);
release validation follows [the publication ADR](../adr/release-validation-before-publication.md) (EP-6); and the
deliberate exclusions listed in Vision & Scope (EP-6, at distillation).

The early representative check is EP-2's third milestone, the fenced-static-member fatal
test. It is the first new integration test, it exercises two consumers, static membership
and both poll modes, and it validates the carried fix before any new API is built on the
harness. If Redpanda does not fence a duplicate `group.instance.id`, EP-2 must find another
reproducible fatal trigger before EP-4 and EP-5 begin, and record it here.


## Progress

No child plan has started. The repository holds the seihou scaffold, `mori.dhall`,
these plans and two review ADRs; there is no Cabal package yet. The review fetched the
source branch and recovered its committed request bundle without switching its checkout. The next action is EP-1. The cross-plan access gate is
the decision on repository visibility before EP-6. Registry refresh is also
outstanding until Mori's database is available. For visibility, the GitHub repository is private, and
downstream `source-repository-package` pins over HTTPS need either public visibility or
authenticated access.

- [ ] Repository visibility decided before EP-6 tags the release.


## Surprises & Discoveries

Review findings (2026-10-21): the fork working tree is on `main`, but the required
source and requests are available at `fe5328a81941e92b93c3f47de14878346c9a5b43`.
Mori registry discovery fails because its PostgreSQL database `mori` is absent; local
`mori show --full` succeeds. Existing cross-repository ADRs were read from their named
checkouts, but registry handle resolution remains unverified. Do not treat registry
refresh as completed until it succeeds.

Source review found missing manual-assignment queue forwarding, a teardown guard that
would skip cleanup, a SASL test expecting an error the C implementation does not return,
and a downstream rename retaining incompatible `>=5.3` bounds. The update corrects
these and moves compatibility proof before publication. No implementation tests have run.


## Decision Log

- Decision: The package is named `rdkafka-hs` and keeps the `Kafka.*` module namespace,
  public type names and record field names of hw-kafka-client 5.3.0.
  Rationale: `mori.dhall` already identifies the project and its single package as
  `rdkafka-hs` with upstream `haskell-works/hw-kafka-client`. Keeping the module namespace
  means every downstream consumer migrates by editing `build-depends`, not imports. The
  record-field conventions from `mori://shinzui/haskell-jitsurei/docs/core-record-patterns`
  are enabled as language extensions but not applied to existing public records, because
  that would break seven known dependents for no behavioural gain; a later initiative may
  do it as a major version.
  Date: 2026-10-21

- Decision: The version line restarts at `1.0.0.0`.
  Rationale: A new package name has no PVP continuity with hw-kafka-client 5.3.0, and
  this author's other libraries start new names at their own first version. The
  changelog's first entry states the lineage (Hackage 5.3.0, the twelve unreleased upstream
  commits, and the fatal-error fix) so nothing is hidden.
  Date: 2026-10-21

- Decision: The fork's git history is imported with `git merge --allow-unrelated-histories`
  rather than copying files.
  Rationale: A maintained fork should preserve upstream attribution and make
  `git log` and `git blame` on inherited code meaningful. Import the reviewed commit, inspect all actual conflicts, and retain the current
  scaffold and plans. Do not assume a fixed conflict list or reset to an old scaffold commit.
  Date: 2026-10-21

- Decision: Supported toolchain is GHC 9.12 and newer only, with `GHC2024`; the primary
  compiler is 9.12.4 and a secondary 9.14.1 shell is only advertised after it is explicitly wired and validated.
  Rationale: The user's standing requirement, matching
  `mori://shinzui/haskell-jitsurei/docs/core-standards`, and what the pinned
  `haskell-nix-dev` toolchain provides (`supportedGhcs = [ "ghc9124" "ghc9141" ]`).
  Date: 2026-10-21

- Decision: The librdkafka floor is 2.15, the version the locked nixpkgs provides, not the
  newest upstream release (2.16.0, 2026-10-06).
  Rationale: The toolchain pin is what every project on this seihou module builds against;
  the binding must compile and be tested there first. Newer headers are additive and EP-3's
  inventory script reports what they add.
  Date: 2026-10-21

- Decision: Integration tests run only against a project-local Redpanda on Apple Container,
  behind a manual cabal flag `integration` that is off by default.
  Rationale: `mori://shinzui/keiro-runtime-kenshou/okf/adrs/concepts/ADR-17` forbids
  mutating the shared broker, and the inherited suite deletes topics. `nix build` runs the
  package's tests in a sandbox with no broker, so the integration suite must not be built
  there. A flag is explicit; skipping on a missing environment variable would hide
  failures.
  Date: 2026-10-21

- Decision: The test framework is `tasty` with `tasty-hunit`, replacing hspec.
  Rationale: This is the framework in the author's `haskell-library` seihou template and in
  `kafka-effectful`. The inherited integration groups are order-dependent, so they are
  ported as `sequentialTestGroup` trees.
  Date: 2026-10-21


Revision note (2026-10-21): reviewed all six children against source and the skill
specifications, recovered the source bundle, clarified integration ownership, corrected
consumer/test contracts and moved downstream proof ahead of publication. The six-plan
decomposition and Not Started statuses remain unchanged.


## Outcomes & Retrospective

(To be filled during and after implementation.)
