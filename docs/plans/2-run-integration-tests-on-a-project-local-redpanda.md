---
id: 2
slug: run-integration-tests-on-a-project-local-redpanda
title: "Run integration tests on a project-local Redpanda"
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
      note: "Changes requested: sync assignment wait needs active polling; enforce private broker guard and cover batch fatal propagation."
  revisions:
    - model: "gpt-6-astra"
      harness: "codex-cli"
      at: 2026-10-22T00:00:20Z
      mode: "update"
      note: "Correct sync fixture polling, private-broker rejection and batch fatal acceptance."
---

# Run integration tests on a project-local Redpanda

This ExecPlan is a living document. The sections Progress, Surprises & Discoveries,
Decision Log, and Outcomes & Retrospective must be kept up to date as work proceeds.
If durable project context changes, update or create ADRs in docs/adr/ in the same change.


## Purpose / Big Picture

After this plan, a developer on an Apple Silicon Mac can type `just test-it` inside the
dev shell and watch a private Redpanda broker start under Apple Container, the package's
integration test-suite run every inherited scenario against it (producing, consuming in
both callback poll modes, committing and storing offsets, batch polling, headers, topic
administration, metadata, seeking), and one new scenario prove the fix the fork was created
for: a consumer fenced by a second static member with the same `group.instance.id` reports
`Left (KafkaResponseError RdKafkaRespErrFatal)` from `pollMessage` in both poll modes,
and `consumerFatalError` names `RdKafkaRespErrFencedInstanceId` as the cause. The broker
is the project's own: it never touches the machine-wide Redpanda at `127.0.0.1:9092`, its
containers and ports are named for this project, and `redpanda-local-down` stops it while
`redpanda-local-purge` deletes it. `nix build` and plain `cabal test` keep passing without
a broker because the integration suite sits behind a manual cabal flag.


## Progress

- [ ] M1: `nix.redpanda` enabled; `redpanda-local-up` starts a broker reachable at
      `127.0.0.1:39092`; `process-compose.override.yaml`, `justfile` recipes and the
      `RDKAFKA_TEST_BROKER` shell variable exist; README documents the flow.
- [ ] M2: `rdkafka-hs-integration` (tasty, flag `integration`) ports every inherited
      hspec scenario and passes against the private broker via `just test-it`;
      `nix build` still passes without it.
- [ ] M3: Fenced-static-member fatal regression test passes in both callback poll modes
      and is the documented producer/consumer check for the harness; ADR-3 (private
      broker for tests) and ADR-4 (fatal errors surfaced in-band) written.


## Surprises & Discoveries

Review (2026-10-21): sync-mode assignment and fencing require active polling
while awaiting callbacks. Added explicit shared-broker rejection and async batch-fatal
acceptance. EP-1 owns the integration flag and disabled stanza; this plan ports them.


## Decision Log

- Decision: The integration suite is a separate cabal test-suite behind the manual flag
  `integration`, default off, rather than one that skips when no broker is configured.
  Rationale: `nix build` runs test-suites in a sandbox without a broker; a flag keeps the
  suite out of that build explicitly, while a skip-on-missing-variable design would make a
  misconfigured environment look green. Recorded in the MasterPlan Decision Log.
  Date: 2026-10-21

- Decision: Broker discovery is the environment variable `RDKAFKA_TEST_BROKER`, defaulting
  to `127.0.0.1:39092`, exported by the dev shell.
  Rationale: Upstream read `KAFKA_TEST_BROKER` defaulting to `localhost:9092`, the shared
  cluster's address, which `mori://shinzui/keiro-runtime-kenshou/okf/adrs/concepts/ADR-17`
  forbids tests from using. The new name and default make the private cluster the only
  path of least resistance.
  Date: 2026-10-21


## Outcomes & Retrospective

(To be filled during and after implementation.)


## Context and Orientation

This plan starts from the package that `docs/plans/1-bootstrap-rdkafka-hs-from-the-hw-kafka-client-fork.md`
produced: `rdkafka-hs.cabal` at the repository root with a `common common-options`
stanza, a unit test-suite `rdkafka-hs-test` under `test/`, the inherited integration
sources under `test-integration/` (still hspec, still named `Kafka.IntegrationSpec` and
`Kafka.TestEnv`, declared as test-suite `rdkafka-hs-integration` behind the manual flag
`integration`), the unmanaged `flake.module.nix`, and a `justfile`.

Redpanda is a Kafka-compatible broker. Apple Container is Apple's macOS container runtime
(the `container` command, installed at `~/.nix-profile/bin/container`); it needs an Apple
Silicon Mac on macOS 26 or newer. This author runs one shared, always-on Redpanda on it at
`127.0.0.1:9092`, managed from the dotfiles. Tests must not use that cluster: the inherited
suite deletes every user topic it can see, and
`mori://shinzui/keiro-runtime-kenshou/okf/adrs/concepts/ADR-17` records the rule that
local Kafka verification uses a disposable, run-owned broker.

The seihou module `nix-haskell-flake` (applied here at version 0.34.0; its owning project is `mori://shinzui/seihou-modules`, project-relative path
`modules/haskell/nix-haskell-flake/`, artifact URI pending; its definition is at `/Users/shinzui/Keikaku/bokuno/seihou-modules/modules/haskell/nix-haskell-flake/`)
has an opt-in variable `nix.redpanda`. When true, it generates `nix/redpanda.nix`, which
reuses the lifecycle scripts of the `redpanda-container` flake (already a locked input of
`flake.nix`) with project-specific names and ports and exposes them in the dev shell as
`redpanda-local-up`, `redpanda-local-down`, `redpanda-local-status`,
`redpanda-local-logs` and `redpanda-local-purge`. The container names become
`rdkafka-hs-rp-0` (broker), `rdkafka-hs-rp-console`, the network `rdkafka-hs-rp`, the
volume `rdkafka-hs-rp-0-data`, and the host ports are the ones already recorded in
`.seihou/manifest.json`: Kafka `39092`, Admin API `39644`, Schema Registry `38081`, HTTP
proxy `38082`, Console `38080` (Console only if `nix.redpanda-console` is also true; leave
it false). On Linux the generated module contributes nothing, so the flake still evaluates
there. `redpanda-local-up` is idempotent and waits for the Admin API readiness probe;
`redpanda-local-status` exits non-zero when the broker is not serving, so it works as a
gate. These behaviours come from the `redpanda-container` repository (project
`mori://shinzui/redpanda-container`, not yet in the local Mori registry; its naming and
port contract is `docs/adr/2-redpanda-container-naming-and-port-contract.md` there).

`process-compose.yaml` is seihou-managed and currently declares no processes; the
unmanaged `process-compose.override.yaml` (an example exists) is merged over it and is
where project processes go. `rpk`, Redpanda's command-line tool, is installed at
`/opt/homebrew/bin/rpk` and is useful for inspection (`rpk topic list -X brokers=127.0.0.1:39092`).

The inherited integration suite, `test-integration/Kafka/IntegrationSpec.hs` (362 lines)
and `test-integration/Kafka/TestEnv.hs` (92 lines), reads `KAFKA_TEST_BROKER` and
`KAFKA_TEST_TOPIC` from the environment with `unsafePerformIO`, generates a random
ten-letter prefix per run for the topic and group names, and defines fixtures
`specWithConsumer`, `specWithProducer`, `specWithKafka` over hspec's `beforeAll` and
`afterAll`. Its scenarios are deliberately ordered (a producer step, then a consumer step
that expects the produced count) and share state through `MVar`s, so they must keep
running sequentially after the port. Its groups are: per-message commit; store offsets
(with `noAutoOffsetStore`); the main consumer group run twice, once in
`CallbackPollModeAsync` and once in `CallbackPollModeSync` (`runConsumerSpec`: receive,
committed, position, watermark offsets, subscription, assignment, metadata, consumer group
info, offsets for time, seeks); batch polling; headers; and topic administration (create,
list, delete). The target framework is tasty 1.5 with tasty-hunit: `describe` becomes
`testGroup`, ordered groups become `sequentialTestGroup AllSucceed`, `it` becomes
`testCase`, `shouldBe` becomes `@?=`, `shouldSatisfy` becomes `assertBool`, and
`beforeAll`/`afterAll` become `withResource`. Tasty's default thread count is the number of
RTS capabilities and the suite is compiled with `-with-rtsopts=-N`, so the top-level tree
must be wrapped in `localOption (NumThreads 1)` as well.

The fatal-error contract this plan tests is described in
`mori://shinzui/keiro/okf/adrs/concepts/ADR-11`: librdkafka delivers a raised consumer
fatal only as a consumer error on the consumer queue, never through `error_cb`; the fork
reads `rd_kafka_fatal_error` on every poll so both poll modes return
`KafkaResponseError RdKafkaRespErrFatal` in-band, and `Kafka.Consumer.consumerFatalError`
returns the underlying cause. The canonical trigger is static membership: a consumer that
sets `group.instance.id` is a static member, and when a second consumer joins the same
group with the same instance id the broker fences the first one, which librdkafka turns
into the fatal `RdKafkaRespErrFencedInstanceId`. librdkafka also exposes
`rd_kafka_test_fatal_error`, a public function that raises an arbitrary fatal for testing,
which is the fallback trigger if Redpanda turns out not to fence.

Local ADR context: EP-1 creates the fork-identity and toolchain-floor records and
adopts [consumer queue and teardown ownership](../adr/consumer-queue-and-teardown-ownership.md).
Use the adopted handles from `okf id list`; allocate new records with `okf id next`,
not by assuming ADR-3 and ADR-4 are free. Sync fixtures must drive callbacks while
waiting for assignment, and cleanup must stay within each test deadline.


## Plan of Work

**Milestone 1: a private broker on demand.** Reconfigure the seihou module with
`nix.redpanda=true`, commit the regenerated `flake.nix`, `nix/redpanda.nix`, `.envrc` and
`.seihou/manifest.json`, and confirm `redpanda-local-up` appears in the shell. Create
`process-compose.override.yaml` with a single `redpanda` process that runs
`redpanda-local-up` and does not restart, so `process-compose up` (the project's generic
"start services" entry point) brings the broker up and exits that process when ready. In
`flake.module.nix` set `haskellProject.shellHook` to export
`RDKAFKA_TEST_BROKER=127.0.0.1:39092` unless already set, with a comment that the port is
the `redpanda.kafka-port` value in `.seihou/manifest.json`. Add `justfile` recipes
`redpanda-up`, `redpanda-down`, `redpanda-status`, `redpanda-logs`, `redpanda-purge` (thin
wrappers), and `test-it` which runs `redpanda-local-up` then
`cabal test rdkafka-hs-integration -f integration`. Fill the README's integration-test
section: requirements (Apple Silicon, macOS 26, Apple Container running), the commands,
the port table, and the statement that the shared cluster is never used.

**Milestone 2: the inherited suite, ported and green.** Replace `test-integration/Spec.hs`
with `test-integration/Main.hs` that builds one `TestTree` under
`localOption (NumThreads 1)` and runs `defaultMain`. Rewrite `Kafka/TestEnv.hs` to read
`RDKAFKA_TEST_BROKER` (default `127.0.0.1:39092`) once in `main`, reject the machine-global loopback endpoint on port 9092
(including localhost and IPv6 aliases) before creating any client, and pass a `TestEnv`
record (broker address, run prefix, functions to make prefixed topic and group names)
into the tree, instead of `unsafePerformIO` globals; keep `consumerProps`,
`producerProps`, the rebalance-wait consumer constructor, and add `withProducer`,
`withConsumer` and `withKafka` built on `withResource`. Split `IntegrationSpec.hs` into
one module per inherited group under `test-integration/Kafka/Integration/`, each exporting
a `TestEnv -> TestTree`, preserving the assertions and their order. Before the first
scenario, fail fast with a clear message if the broker does not answer a metadata request
within ten seconds, naming `just redpanda-up` as the fix. Keep the per-run prefix on every
topic and group name so repeated runs on the same broker do not interfere; the topic
administration group may delete only topics carrying the run prefix. Update
`rdkafka-hs.cabal`: the integration suite's `other-modules`, its dependencies (`tasty`,
`tasty-hunit`, `rdkafka-hs`, `bytestring`, `containers`, `text`, `random`; drop `hspec`,
`hspec-discover`, `either`, `monad-loops`). Acceptance: `just test-it` passes; `nix build`
passes; `cabal build all` without the flag does not build the suite.

**Milestone 3: the carried fix, proven.** Add `test-integration/Kafka/Integration/FatalErrorTest.hs`
with a group that runs once per callback poll mode. Each run: create a topic with the run
prefix; create consumer A with a fresh prefixed group id, `group.instance.id` set to a
prefixed value through `extraProps`, `noAutoCommit`, and a rebalance callback that
signals assignment; drive polling while waiting with a deadline (especially in
sync mode, where waiting only on the callback signal would deadlock); create consumer B with the same group id and
instance id; actively poll both clients while waiting for fencing; poll A repeatedly (in sync mode the poll is also what drives librdkafka's
callbacks) until `pollMessage` returns `Left (KafkaResponseError RdKafkaRespErrFatal)` or
thirty seconds pass; assert the fatal arrived, assert a second poll returns the same
value, and assert `consumerFatalError a` returns
`Just (KafkaResponseError RdKafkaRespErrFencedInstanceId, description)` with a non-empty
description; also assert async `pollMessageBatch` repeatedly reports the singleton fatal
error; close both consumers. If the fence does not arrive within the deadline on
Redpanda, record the observation in Surprises & Discoveries, bind
`rd_kafka_test_fatal_error` in `src/Kafka/Internal/RdKafka.chs`, expose it from a new
exposed module `Kafka.Internal.Testing` documented as test support only, and make the test
raise the fatal that way, keeping the static-membership scenario as a second, skippable
case. Write `docs/adr/3-integration-tests-run-against-a-project-local-redpanda.md`
(private cluster, ports, flag, no shared broker, Darwin only) and
`docs/adr/4-consumer-fatal-errors-are-surfaced-in-band.md` (the library-side statement of
the contract in keiro's ADR-11: in-band `RdKafkaRespErrFatal` from every poll in both
modes, cause from `consumerFatalError`, the background loop destroys what it polls, and
the documented limitation that `max.poll.interval.ms` cannot detect a stuck application
in async mode). Append the CHANGELOG entry for the test harness.


## Concrete Steps

All commands run from `/Users/shinzui/Keikaku/bokuno/rdkafka-hs` inside the dev shell
(`nix develop`, or direnv).

Enable the private broker:

```bash
seihou run nix-haskell-flake --var nix.redpanda=true --diff
seihou run nix-haskell-flake --var nix.redpanda=true
git add flake.nix nix/redpanda.nix .envrc .seihou/manifest.json
git commit -m "chore(nix): add project-local Redpanda scripts" \
  --trailer "MasterPlan: docs/masterplans/1-fork-hw-kafka-client-as-rdkafka-hs.md" \
  --trailer "ExecPlan: docs/plans/2-run-integration-tests-on-a-project-local-redpanda.md" \
  --trailer "Intention: intention_01m5gc388wecfsq431s3pgg1dd"
direnv reload            # or re-enter nix develop
which redpanda-local-up  # expect a /nix/store path
redpanda-local-up
```

Expected tail of the `redpanda-local-up` output:

```text
Redpanda is ready.

  Kafka            127.0.0.1:39092
  Admin API        127.0.0.1:39644
  Schema Registry  127.0.0.1:38081
  HTTP Proxy       127.0.0.1:38082
```

Then confirm from outside the test-suite:

```bash
rpk cluster info -X brokers=127.0.0.1:39092
redpanda-local-status
```

Create `process-compose.override.yaml`:

```yaml
version: "0.5"
processes:
  redpanda:
    command: redpanda-local-up
    availability:
      restart: "no"
```

Add to `flake.module.nix` inside `perSystem`:

```nix
# Private test broker; the port is redpanda.kafka-port in .seihou/manifest.json.
haskellProject.shellHook = ''
  export RDKAFKA_TEST_BROKER="''${RDKAFKA_TEST_BROKER:-127.0.0.1:39092}"
'';
```

Port the suite (Milestone 2), then run it:

```bash
just test-it
```

Expected: the run ends with a line like `All 38 tests passed (…s)`; the exact count is
whatever the port yields and should be recorded in Progress. Confirm the flag gate:

```bash
cabal build all 2>&1 | grep -c rdkafka-hs-integration   # expect 0
nix build
```

Add the fatal test (Milestone 3) and run only it while iterating:

```bash
cabal test rdkafka-hs-integration -f integration --test-options='-p "/fatal/"'
```

Validate the ADRs and the bundle log:

```bash
okf validate docs/adr --strict --profile docs/adr/profile.dhall --profile-enforce --log-enforce
```

Every commit carries:

```text
MasterPlan: docs/masterplans/1-fork-hw-kafka-client-as-rdkafka-hs.md
ExecPlan: docs/plans/2-run-integration-tests-on-a-project-local-redpanda.md
Intention: intention_01m5gc388wecfsq431s3pgg1dd
```


## Validation and Acceptance

With the Apple Container service running, `just test-it` from a clean checkout starts (or
finds) the broker and prints a passing tasty summary that includes groups named for every
inherited scenario and a `fatal` group with two cases, `async` and `sync`. During the run,
`container list` shows `rdkafka-hs-rp-0` and nothing named `redpanda-0` is created or
stopped; configuration tests prove the shared endpoint is rejected before network access;
the suite never needs to connect to the shared broker for acceptance. With the broker
stopped (`redpanda-local-down`), `cabal test rdkafka-hs-integration -f integration` fails
within fifteen seconds including failure cleanup, with a message that names `just redpanda-up`. `nix build` succeeds
without a broker. `RDKAFKA_TEST_BROKER=127.0.0.1:1 just test-it` fails fast rather than
hanging.

An explicit override naming localhost:9092, 127.0.0.1:9092 or [::1]:9092 must
fail before connecting; verify this with configuration-level tests.

The fatal test is the harness's representative producer/consumer check and must pass
before `docs/plans/4-expose-queue-event-notification-for-consumers.md` or
`docs/plans/5-support-a-bounded-assign-only-observer-consumer.md` begin. Its evidence (the tasty lines for
both poll modes and the recovered cause) belongs in this plan's Progress entry for M3.


## Idempotence and Recovery

`seihou run` with the same variables is a no-op. `redpanda-local-up` is idempotent and
safe to run at any time; `redpanda-local-down` keeps data; `redpanda-local-purge --force`
deletes the containers, network and volume and is the reset when the broker's state is
suspect (for example after many aborted runs left stale groups). If `redpanda-local-up`
reports that the Apple Container service is not responding, run
`container system start`, or `container system stop && container system start` when
networking is wedged. Test runs are isolated by the random run prefix, so a failed run
needs no cleanup before the next; the topic-administration group deletes its own prefixed
topics. Porting the suite is ordinary editing; the hspec sources remain in git history if a
scenario needs to be re-read.


## Interfaces and Dependencies

Prerequisite: `docs/plans/1-bootstrap-rdkafka-hs-from-the-hw-kafka-client-fork.md` is
Complete (the package builds, `flake.module.nix` and `justfile` exist, the integration
suite stanza exists behind flag `integration`).

Services: Apple Container and the `redpanda-container` lifecycle scripts via
`nix/redpanda.nix` (Darwin only). Libraries added to the integration suite: `tasty ^>=1.5`,
`tasty-hunit ^>=0.10`, `random`.

Artifacts defined here that later plans use: `test-integration/Main.hs` (register new
groups here), `test-integration/Kafka/TestEnv.hs` with the `TestEnv` record and the
`withProducer`, `withConsumer`, `withKafka` fixtures and the prefixed-name helpers, the
`RDKAFKA_TEST_BROKER` variable and its `127.0.0.1:39092` default, the `just test-it`
recipe, and the optional `Kafka.Internal.Testing` module if the fallback trigger was
needed. `docs/plans/3-refresh-the-librdkafka-bindings-for-2-15-and-newer.md`,
`docs/plans/4-expose-queue-event-notification-for-consumers.md` and
`docs/plans/5-support-a-bounded-assign-only-observer-consumer.md` add modules under
`test-integration/Kafka/Integration/` and must use the run prefix.
