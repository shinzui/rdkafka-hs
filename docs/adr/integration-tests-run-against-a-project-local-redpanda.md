---
type: Architecture Decision Record
title: Integration tests run against a project-local Redpanda
description: The integration suite runs only against this project's private Redpanda on Apple Container, behind a manual Cabal flag, and refuses the machine-global broker.
timestamp: 2026-10-22T00:36:35Z
generated:
  by: claude-code/claude-opus-5-5
  at: "2026-10-22T00:36:35Z"
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

## Consequences

Integration tests run only on Apple Silicon Macs; Linux broker orchestration and
continuous integration are outside the first release. Tests added by later work
register in `test-integration/Main.hs`, use the `TestEnv` fixtures and run prefix,
and never hard-code port 9092. Repeated or aborted runs on the same broker do not
interfere; `just redpanda-purge` resets the broker's state when needed.
