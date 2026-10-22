---
id: 3
slug: refresh-the-librdkafka-bindings-for-2-15-and-newer
title: "Refresh the librdkafka bindings for 2.15 and newer"
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
      note: "Changes requested: SASL negative test contradicts implementation; inventory traverses prohibited store paths and weakens configure-time floor."
  revisions:
    - model: "gpt-6-astra"
      harness: "codex-cli"
      at: 2026-10-22T00:00:20Z
      mode: "update"
      note: "Correct SASL/error ownership, network test placement and source-based binding inventory."
    - model: "claude-opus-5-5"
      harness: "claude-code"
      at: 2026-10-22T00:39:17Z
      mode: "implement"
      note: "Implement EP-3: librdkafka floor and version API, binding inventory, lifecycle bindings."
---

# Refresh the librdkafka bindings for 2.15 and newer

This ExecPlan is a living document. The sections Progress, Surprises & Discoveries,
Decision Log, and Outcomes & Retrospective must be kept up to date as work proceeds.
If durable project context changes, update or create ADRs in docs/adr/ in the same change.


## Purpose / Big Picture

After this plan, the package states and enforces which librdkafka it supports, tells the
application which librdkafka it is linked against, documents exactly which of librdkafka's
public C functions are bound and which are not, and binds the handful of client-lifecycle
functions that upstream never did. Concretely: `cabal build` fails at configure time with
a clear message if the installed librdkafka is older than 2.15; a program can print
`Kafka.Version.librdkafkaVersion` and see `2.15.0`; `docs/bindings.md` lists every
`rd_kafka_*` function in the pinned header grouped by the header's own sections, marking
each as bound or unbound, and `just bindings-check` fails when that file is stale; and
applications gain `flushProducerTimeout`, `purgeProducer`, `assignmentLost`,
`consumerClosed` and `setSaslCredentials`, each exercised by a test. The conventions this
plan fixes for `src/Kafka/Internal/RdKafka.chs` are what the next two plans follow when
they add their own bindings.


## Progress

- [x] M1 (2026-10-22): `pkgconfig-depends: rdkafka >=2.15` replaces `extra-libraries`; `Kafka.Version`
      exposes the linked library's version; a unit test asserts it is at least 2.15.0;
      `nix build` still succeeds. Evidence: `cabal repl` prints
      `("2.15.0",LibrdkafkaVersion {versionMajor = 2, versionMinor = 15, versionRevision = 0})`;
      with the bound temporarily `>=99` Cabal stops with `pkg-config package
      rdkafka>=99, not found in the pkg-config database`; unit suite `All 10 tests
      passed`; `nix build` passes. Commit `6d5bc87`.
- [x] M2 (2026-10-22): `scripts/binding-inventory.sh` regenerates `docs/bindings.md` from the pinned
      header and the c2hs module; `just bindings-check` fails on a stale inventory and
      passes on a fresh one; the binding module's section conventions are documented at
      the top of the file. Evidence: header from the `v2.15.0` tag (commit
      `9a94e11452cdeb0a844db44ee5dd01ccbe17d3ab`, sha256
      `33cca14d9fc87117100ce90bd84f2006b8ba521c788055db6af614d0f2f7ca2f`); 495 exported
      functions (500 `RD_EXPORT` matches less five preprocessor lines); with
      `rd_kafka_version_str`'s directive disabled, `just bindings-check` prints a diff
      and exits 1. Commit `b940ae6`.
- [x] M3 (2026-10-22): `flushProducerTimeout`, `purgeProducer`, `assignmentLost`, `consumerClosed` and
      `setSaslCredentials` exist, are documented, and pass their tests (pure unit tests and lifecycle integration tests); ADR-4 amended with the
      librdkafka floor rationale; CHANGELOG updated. Evidence: `just test-it` reports
      `All 73 tests passed (42.21s)`, including the `lifecycle` group (the flush against
      `127.0.0.1:1` returns `RdKafkaRespErrTimedOut` in `0.50s`, the purge empties the
      queue and reports `RdKafkaRespErrPurgeQueue` to the delivery callback) and
      `assignment is not lost` in both poll modes; `docs/bindings.md` shows 131 of 495
      bound with all seven named functions bound and `rd_kafka_queue_io_event_enable`
      unbound; `just check` passes.


## Surprises & Discoveries

Review (2026-10-21): verified the locked nixpkgs package expression declares
2.15.0 and upstream release tags include 2.16.0. The v2.15.0 SASL implementation
accepts non-null credentials even without SASL configured. Inventory inputs now come
from a verified source checkout outside the Nix store, and configure-time enforcement
cannot be replaced by a unit test.

Implementation (2026-10-22):

- `rd_kafka_version` and `rd_kafka_version_str` were already bound (as `pure`
  c2hs functions); M1 only added the public module. Before this plan the module
  bound 113 functions by the inventory's count (c2hs `fun` and `call` directives
  plus hand-written imports and finalizer pointers), not 99; it now binds 131.
- `rdkafka.pc` lists librdkafka's own libraries under `Requires.private`, and
  Cabal runs `pkg-config --cflags`, which needs their `.pc` files. The sandboxed
  `nix build` had them; the dev shell did not, so `flake.module.nix` adds
  `map lib.getDev pkgs.rdkafka.buildInputs` to `haskellProject.extraBuildInputs`.
- The Mori corpus checkout of librdkafka is at a merge commit, not the release
  commit, although its `src/rdkafka.h` is byte-identical to `v2.15.0`'s. The
  inventory records the header's sha256, and `just librdkafka-source` clones the
  tag matching `pkg-config --modversion rdkafka` into `~/.cache/rdkafka-hs/`.
- Admin-API names contain capitals (`rd_kafka_event_CreateTopics_result`), and a
  comment quoting a `{#fun ...#}` example was read as a binding; both were fixed in
  the script and the comment.
- `closeProducer` only flushes, so a test producer pointed at an unreachable broker
  must purge before closing or it waits forever; `closeProducer` still never
  destroys the client (left to its finalizer).


## Decision Log

- Decision: The librdkafka floor is 2.15, the version the locked nixpkgs provides, and it
  is enforced through `pkgconfig-depends` rather than documented only.
  Rationale: Recorded in the MasterPlan: the Nix pin is what every project on this module
  builds against. `pkgconfig-depends` gives a configure-time check and lets cabal2nix add
  `pkg-config` automatically; librdkafka ships `rdkafka.pc`.
  Date: 2026-10-21

- Decision: The inventory uses a verified upstream header matching the linked library version,
  held outside `/nix/store`.
  Rationale: Repository instructions prohibit reading or traversing `/nix/store`. The
  corpus `mori://confluentinc/librdkafka` may lag or lead the release. Record its exact
  verified release tag, commit and header hash, and reject a version mismatch with
  `pkg-config --modversion rdkafka`.
  Date: 2026-10-21


- Decision: `Kafka.Version` exposes `LibrdkafkaVersion` with prefixed fields
  (`versionMajor`, `versionMinor`, `versionRevision`), and `setSaslCredentials` is
  defined in both `Kafka.Consumer` and `Kafka.Producer`.
  Rationale: Prefixed fields match the library's existing records and avoid
  exporting generic names; `HasKafka` is internal, and same-named functions in the
  two modules follow `brokersList` and `setCallback`.
  Date: 2026-10-22

- Decision: `just bindings-check` is part of `just check`, using a cached clone of
  the release tag unless `RDKAFKA_SOURCE_HEADER` is set.
  Rationale: The check must run without per-machine setup; the first run needs
  network access once.
  Date: 2026-10-22


## Outcomes & Retrospective

Completed 2026-10-22. The package rejects librdkafka older than 2.15 at configure
time, reports the linked version through `Kafka.Version`, and documents its coverage
in `docs/bindings.md` (131 of 495 exported functions), kept honest by
`just bindings-check` in `just check`. Producers gain a bounded flush and a purge,
consumers gain `assignmentLost` and `consumerClosed`, and both gain
`setSaslCredentials`; `flushProducer` now uses `rd_kafka_flush`. ADR-4 records the
enforcement and how newer releases are adopted.

For EP-4: follow the conventions comment at the top of
`src/Kafka/Internal/RdKafka.chs` and run `just bindings-inventory` after adding the
queue event bindings.


## Context and Orientation

The package `rdkafka-hs` at the repository root binds librdkafka, the C client for Apache
Kafka, through c2hs. c2hs is a preprocessor: `src/Kafka/Internal/RdKafka.chs` contains
ordinary Haskell plus directives such as `{#fun rd_kafka_subscribe as ^ {...} -> ...#}`,
`{#enum rd_kafka_resp_err_t as RdKafkaRespErrT ...#}` and `{#pointer *rd_kafka_t as
RdKafkaTPtr foreign -> RdKafkaT#}`, which it expands into FFI imports by reading the C
header `librdkafka/rdkafka.h`. The header comes from the dev shell: the seihou option
`nix.kafka` (enabled by EP-1) adds `pkgs.rdkafka` and `pkgs.rdkafka.dev` and exports
`CPATH`, `LIBRARY_PATH` and `PKG_CONFIG_PATH`, so `pkg-config --modversion rdkafka` prints
`2.15.0` and `pkg-config --variable=includedir rdkafka` prints the include directory
holding `librdkafka/rdkafka.h`. The locked nixpkgs is `d5dfd8e6716dde34398bc14bc87c10dece9c8c68`
through the `haskell-nix-dev` input. Upstream librdkafka's newest releases are 2.16.0
(2026-10-06), 2.15.1 (2026-09-09) and 2.15.0 (2026-06-30); a readable checkout at 2.15.0
is at `/Users/shinzui/Keikaku/hub/librdkafka-project/librdkafka` (Mori project
`mori://confluentinc/librdkafka`), whose `src/rdkafka.h` carries five hundred `RD_EXPORT`
declarations.

The c2hs module is about 1400 lines and binds 99 C functions today, counting both
`{#fun ...#}` declarations and inline `{#call ...#}` uses. It already covers the
configuration API, the high-level consumer (subscribe, assign, incremental assign, poll,
commit, committed, position, seek, pause and resume, offsets store), the producer
(`rd_kafka_produceva`, `rd_kafka_produce_batch`), transactions, metadata and group
listing, headers, `rd_kafka_fatal_error`, and the admin calls for creating and deleting
topics. It does not bind `rd_kafka_version`, `rd_kafka_version_str`, `rd_kafka_flush`
(`Kafka.Producer.flushProducer` loops on `rd_kafka_outq_len` and `rd_kafka_poll` instead),
`rd_kafka_purge`, `rd_kafka_assignment_lost`, `rd_kafka_consumer_closed`,
`rd_kafka_sasl_set_credentials`, the queue event functions (EP-4 binds those), nor any
admin operation beyond topics. Enumerations declared with `{#enum ...#}` are regenerated
from the header on every build, so new `rd_kafka_resp_err_t` codes appear in
`RdKafkaRespErrT` automatically; nothing in this plan needs to hand-maintain them.

Pointer and finalizer conventions in the module today: handles that librdkafka expects the
caller to destroy are declared as `foreign` pointers and wrapped by a `newRdKafka...`
helper that attaches the destroy function as a finalizer (`newRdKafkaTopicPartitionListT`,
`newRdKafkaTopicConfT`, `newRdKafkaT`, which attaches `rd_kafka_destroy_flags` with
`RD_KAFKA_DESTROY_F_NO_CONSUMER_CLOSE` through `Foreign.Concurrent` because the finalizer
may re-enter Haskell). Functions returning `rd_kafka_error_t *` are turned into
an error code with `rdKafkaErrorCode`, then into `Either KafkaError ()` or
`Maybe KafkaError` using `Kafka.Internal.Shared` helpers. Those helpers accept enum
codes and do not free error objects: bracket each non-null object with
`rdKafkaErrorDestroy`, and handle null success before dereferencing. String results are read with
`peekCText` into `Text`. `Kafka.Internal.Setup` wraps the raw handles in `Kafka`,
`KafkaConf` and `TopicConf` and provides the `HasKafka` class used by both
`Kafka.Consumer.Types.KafkaConsumer` and `Kafka.Producer.Types.KafkaProducer`.

The producer's `flushProducer :: MonadIO m => KafkaProducer -> m ()` in
`src/Kafka/Producer.hs` blocks until the out queue is empty and is also what
`closeProducer` calls. `Kafka.Consumer.closeConsumer` in `src/Kafka/Consumer.hs` takes the
callback-poll status `MVar`, destroys the message queue and calls
`rd_kafka_consumer_close`.

Tests: the unit suite `rdkafka-hs-test` (tasty, `test/`) needs no broker; the integration
suite `rdkafka-hs-integration` (tasty, `test-integration/`, flag `integration`) runs
against the project-local Redpanda at `RDKAFKA_TEST_BROKER` (default `127.0.0.1:39092`)
through `just test-it`, with fixtures in `test-integration/Kafka/TestEnv.hs` and one module
per group under `test-integration/Kafka/Integration/`, all from EP-2.

ADR context: ADR-4, `docs/adr/toolchain-floor-is-ghc-9-12-and-librdkafka-2-15.md` (from EP-1),
states the floor; this plan amends it with the enforcement mechanism and the policy on
newer headers. No other local or cross-repository ADR constrains this plan.


## Plan of Work

**Milestone 1: floor and version.** In `rdkafka-hs.cabal` replace the library's
`extra-libraries: rdkafka` with `pkgconfig-depends: rdkafka >=2.15`. Add
`src/Kafka/Version.hs`, exposed, with `librdkafkaVersion :: Text` (from
`rd_kafka_version_str`) and `librdkafkaVersionNumber :: Int` (from `rd_kafka_version`,
the hex-encoded `0xMMmmrrpp`), both bound in the c2hs module as `pure` functions since
they are constants of the linked library, plus a small decoded view
`LibrdkafkaVersion { major, minor, revision }` with a function from the number. Add a
unit test in `test/Kafka/VersionTest.hs` asserting the number is at least `0x020f0000`
and that the string's decoded major and minor match the number. Mention the version
function in the README's requirements section. Confirm `nix build` still works: cabal2nix
maps `pkgconfig-depends` to a `pkg-config` native build input and the `rdkafka` package
argument, which `nix/haskell.nix` resolves from nixpkgs without changes.

**Milestone 2: the inventory.** Write `scripts/binding-inventory.sh` (POSIX shell plus
`awk`, both in the shell) that: locates the header through an explicit `RDKAFKA_SOURCE_HEADER` path to a verified release
checkout outside `/nix/store` (resolve symlinks and reject store paths before opening); records `pkg-config --modversion rdkafka`; captures doxygen group headings before stripping comments, then normalizes the
header by joining each declaration onto one line so multi-line
`RD_EXPORT` prototypes are not missed (the naive single-line regex finds 368 of the 500
exports); extracts every `rd_kafka_[A-Za-z0-9_]*` function name together with the nearest
preceding doxygen `@name` group heading; extracts the bound names from
`src/Kafka/Internal/RdKafka.chs` (`{#fun NAME`, `{# fun NAME`, `{#call NAME`, `{# call
NAME`); and writes `docs/bindings.md` with a header stating the librdkafka version and
counts, then one section per header group with a two-column list of bound functions and
their Haskell names, and the unbound names in a collapsed block. Functions bound in the
module but absent from the header are listed at the end as "stale" and make the script
exit non-zero, because such a binding would also fail to compile. Add `justfile` recipes
`bindings-inventory` (regenerate) and `bindings-check` (regenerate to a temporary file and
`diff` against the committed one). Add a short conventions comment at the top of the
`.chs` module: sections mirror the header's `@name` groups in header order; plain calls use
`{#fun ... as ^#}`; anything needing buffers, finalizers or error-object handling gets a
hand-written wrapper directly below its `{#fun#}`; new bindings go in the section of the
function's header group; and every change to the file is followed by
`just bindings-inventory`. Acceptance: `just bindings-check` passes on the committed tree,
and fails after temporarily removing an existing unique binding from an inventory input
copy without regenerating. Adding another version binding would not change the bound set.
Also scan handwritten `foreign import ccall` symbols; report destroy/finalizer bindings
and aliases rather than treating c2hs directives as the entire FFI surface.

**Milestone 3: lifecycle bindings.** Bind and expose, with haddocks that explain the
librdkafka semantics:

`rd_kafka_flush` as `Kafka.Producer.flushProducerTimeout :: MonadIO m => KafkaProducer ->
Timeout -> m (Maybe KafkaError)`, returning `Just (KafkaResponseError
RdKafkaRespErrTimedOut)` when messages remain; reimplement `flushProducer` as repeated
one-second `flushProducerTimeout` calls until it returns `Nothing`, preserving its
block-until-empty contract and still serving delivery callbacks (flush polls internally).

`rd_kafka_purge` as `Kafka.Producer.purgeProducer :: MonadIO m => KafkaProducer ->
[PurgeFlag] -> m (Maybe KafkaError)` with `data PurgeFlag = PurgeQueue | PurgeInFlight |
PurgeNonBlocking` mapping to `RD_KAFKA_PURGE_F_QUEUE`, `RD_KAFKA_PURGE_F_INFLIGHT`,
`RD_KAFKA_PURGE_F_NON_BLOCKING`; purged messages are reported to the delivery callback
with `RdKafkaRespErrPurgeQueue` or `RdKafkaRespErrPurgeInflight`, which the haddock must
say.

`rd_kafka_assignment_lost` as `Kafka.Consumer.assignmentLost :: MonadIO m =>
KafkaConsumer -> m Bool`, meaningful inside a rebalance callback.

`rd_kafka_consumer_closed` as `Kafka.Consumer.consumerClosed :: MonadIO m =>
KafkaConsumer -> m Bool`.

`rd_kafka_sasl_set_credentials` as `Kafka.Types.setSaslCredentials :: (MonadIO m,
HasKafka k) => k -> Text -> Text -> m (Maybe KafkaError)` (username, password), using null-success handling and bracketed error-code extraction/destruction; `HasKafka` lives in `Kafka.Internal.Setup`, so either
re-export the class from a public module or place the function in `Kafka.Consumer` and
`Kafka.Producer` as two thin wrappers. Choose the latter if `HasKafka` is not already
public.

Tests. Keep broker-free version checks in the unit suite. Put the following
network-using lifecycle tests in the integration suite, since loopback listening and
connection behavior may be unavailable in a Nix sandbox. A producer configured with `bootstrap.servers` pointing at a
closed loopback port produces one message, `flushProducerTimeout (Timeout 500)` returns
the timed-out error, `purgeProducer [PurgeQueue]` returns `Nothing` and a subsequent
`flushProducerTimeout (Timeout 100)` returns `Nothing` because the queue is empty; a
consumer with no subscription on the same unreachable address reports `consumerClosed` `False`, then `True`
after `closeConsumer`; `setSaslCredentials` with non-null username/password on a client without SASL
configured returns `Nothing`: upstream v2.15.0 stores the credentials and returns NULL
without checking the security protocol. Test successive credential replacement, not
a fabricated invalid-argument result. A raw null-argument error test belongs only at
the C binding boundary; the public Text API cannot represent null pointers. Integration: in the
inherited consumer group, `assignmentLost` is `False` after assignment; `flushProducer`
still delivers the two-message scenario. Append the CHANGELOG entries and amend ADR-4 with
the `pkgconfig-depends` enforcement and the rule that newer librdkafka releases are
adopted by moving the Nix pin and rerunning the inventory.


## Concrete Steps

All commands run from `/Users/shinzui/Keikaku/bokuno/rdkafka-hs` inside the dev shell.

Confirm the header and version the build will see:

```bash
pkg-config --modversion rdkafka                                   # 2.15.0
# After Mori discovery, use a release checkout outside /nix/store for inventory input.
rdkafka_source_dir=$(mktemp -d)
git clone --depth 1 --branch v2.15.0 https://github.com/confluentinc/librdkafka.git "$rdkafka_source_dir"
git -C "$rdkafka_source_dir" rev-parse HEAD  # verify against upstream release tag
export RDKAFKA_SOURCE_HEADER="$rdkafka_source_dir/src/rdkafka.h"
```

After Milestone 1:

```bash
cabal build rdkafka-hs
cabal test rdkafka-hs-test
cabal repl rdkafka-hs --repl-options=-e --repl-options='Kafka.Version.librdkafkaVersion'
nix build
```

The repl line prints `"2.15.0"`. To see the floor enforced, temporarily edit the bound to
`>=99` and run `cabal build`; cabal must stop at configure with a message naming
`rdkafka` and the version it found; revert the edit.

After Milestone 2:

```bash
just bindings-inventory
git add docs/bindings.md scripts/binding-inventory.sh
just bindings-check && echo fresh
head -12 docs/bindings.md
```

The head shows the librdkafka version, the number of exported functions found (expect
around 500, matching `grep -c RD_EXPORT` on the header within a handful), and the number
bound (record actual counts after version and lifecycle bindings; do not hard-code 99/106).

After Milestone 3:

```bash
cabal build all -f examples   # require success and inspect for warnings
cabal test rdkafka-hs-test
just test-it
just bindings-check
okf validate docs/adr --strict --profile docs/adr/profile.dhall --profile-enforce --log-enforce
```

Every commit carries:

```text
MasterPlan: docs/masterplans/1-fork-hw-kafka-client-as-rdkafka-hs.md
ExecPlan: docs/plans/3-refresh-the-librdkafka-bindings-for-2-15-and-newer.md
Intention: intention_01m5gc388wecfsq431s3pgg1dd
```


## Validation and Acceptance

Accepted when: `cabal test rdkafka-hs-test` passes including the version test;
`just test-it` passes including producer lifecycle, SASL and `assignmentLost` cases;
`just bindings-check` passes and `docs/bindings.md` names librdkafka 2.15.0, lists
`rd_kafka_flush`, `rd_kafka_purge`, `rd_kafka_assignment_lost`, `rd_kafka_consumer_closed`,
`rd_kafka_sasl_set_credentials`, `rd_kafka_version` and `rd_kafka_version_str` as bound,
and lists `rd_kafka_queue_io_event_enable` as unbound (EP-4 changes that); `nix build`
passes; and a program linked against the package can print the librdkafka version. The
behaviour beyond compilation is visible in the integration test transcript: a flush against an
unreachable broker times out in about half a second instead of hanging, and a purge empties
the queue.

Required acceptance is M1 through M3 as written. Binding further admin operations, the
mock cluster API, or interceptors is explicitly not part of this plan; the inventory makes
the gap visible for a later decision.


## Idempotence and Recovery

Every step is an ordinary edit plus a rebuild. `scripts/binding-inventory.sh` is a pure
function of the header and the c2hs module and can be rerun at will. If
`pkgconfig-depends` breaks `nix build` because cabal2nix cannot find `rdkafka`, fix package resolution through the unmanaged `flake.module.nix` application
overlay while retaining `pkgconfig-depends`. A unit test is not a configure-time check
and cannot replace the declared floor. Do not patch managed `nix/haskell.nix`.


## Interfaces and Dependencies

Prerequisites: `docs/plans/1-bootstrap-rdkafka-hs-from-the-hw-kafka-client-fork.md`
Complete for all milestones; `docs/plans/2-run-integration-tests-on-a-project-local-redpanda.md`
Complete before Milestone 3 begins, because its integration checks run through
`just test-it` and the fixtures in `test-integration/Kafka/TestEnv.hs`.

Libraries: `c2hs` 0.28.8 (build tool), librdkafka 2.15.0 through `pkg-config`, `tasty`
and `tasty-hunit` for tests, and `network` in the integration suite for closed-loopback-port tests if a
plain unused port number is not reliable enough.

Artifacts later plans consume: the section conventions and the inventory check for
`src/Kafka/Internal/RdKafka.chs` (EP-4 adds the queue event bindings and must regenerate
`docs/bindings.md`); `consumerClosed` (only valid before destruction);
`Kafka.Version` (EP-6 cites it in the README); `just bindings-check` (EP-6 runs it before
tagging).
