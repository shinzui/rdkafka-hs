---
type: Architecture Decision Record
title: Integration tests run against a project-local Redpanda
description: The integration suite uses a private primary Redpanda or explicitly selected run-owned Apache processes and refuses shared broker mutation.
timestamp: 2026-10-25T01:04:47Z
generated:
  by: codex-cli/gpt-6.1-sol
  at: "2026-10-25T01:04:47Z"
docId: ADR-5
status: Accepted
date: 2026-10-21
originatingPlan: docs/plans/2-run-integration-tests-on-a-project-local-redpanda.md
---

# Integration tests run against a project-local Redpanda

## Context

The inherited integration suite read `KAFKA_TEST_BROKER`, defaulting to
`localhost:9092`, and deleted every user topic it could see. On this author's
machines `127.0.0.1:9092` is a shared, always-on Redpanda that other projects use.
`mori://shinzui/keiro-runtime-kenshou/okf/adrs/concepts/ADR-17` records the rule
that local Kafka verification uses a disposable, run-owned broker and never mutates
the machine-global one. `nix build` runs the package's test-suites in a sandbox
with no broker at all.

## Decision

The integration suite `rdkafka-hs-integration` is a separate Cabal test-suite that
is buildable only with the manual flag `integration`, default off. It is not
skipped at run time when a broker is missing: a flag keeps it out of `nix build`
explicitly, while a skip would make a misconfigured environment look green.

It runs against a private Redpanda owned by this project, started by the
`nix.redpanda` option of the seihou `nix-haskell-flake` module through the
`redpanda-container` lifecycle scripts on Apple Container (Apple Silicon macOS
only). Its containers are named `rdkafka-hs-rp-*` and its host ports are the
`redpanda.*-port` values in `.seihou/manifest.json`: Kafka `39092`, Admin API
`39644`, Schema Registry `38081`, HTTP proxy `38082`. Changing a port means changing
the manifest, `flake.module.nix`, `test-integration/Kafka/TestEnv.hs` and
`README.md` together.

The suite reads its broker from `RDKAFKA_TEST_BROKER`, default `127.0.0.1:39092`,
which the dev shell exports. Before creating any client it rejects a loopback
address on port 9092, or a loopback address without a port, and it fails within
ten seconds, naming `just redpanda-up`, when the broker does not answer metadata.
Every topic and consumer group carries a random per-run prefix, and topic
administration deletes only this run's topics. `just test-it` starts the broker if
needed and runs the suite.

Expanded qualification has two explicit Tasty entry points: `--broker-qualification`
creates three Apache broker/controller processes, and `--authentication-qualification`
creates one SASL/TLS broker with ephemeral identities and ACLs. Both require
`RDKAFKA_TEST_KAFKA_HOME`; missing prerequisites fail rather than skip. One shared
fixture owns reserved ephemeral loopback ports, data directories, bounded command
processes and broker ProcessHandles. It signals only handles returned by its own
createProcess calls. Security supplies configuration/lifecycle hooks to that runner;
it does not introduce a separate process owner. The secure public listener verifies
its generated CA and hostname. Internal loopback health/controller listeners are
fixture plumbing, not a production deployment recommendation.

Each fixture directory is private (0700). Credentials/configuration are private
files (0600); configuration dumps are disabled. Cleanup stops all owned processes,
removes broker state and security material, and checks retained textual logs for
ephemeral passwords. Failed/interrupted runs retain sanitized diagnostics and are
not accepted as successful recovery. Primary Redpanda and shared machine-global
resources are never stopped or reconfigured by these entry points.

## Consequences

The first release demonstrated the primary Apple Silicon fixture. Expanded
qualification additionally runs actual Linux executables and explicitly selected
Apache Kafka distributions. Continuous integration remains outside this change. Tests added by later work
register in `test-integration/Main.hs`, use the `TestEnv` fixtures and run prefix,
and never hard-code port 9092. Repeated or aborted runs on the same broker do not
interfere; `just redpanda-purge` resets the broker's state when needed.
