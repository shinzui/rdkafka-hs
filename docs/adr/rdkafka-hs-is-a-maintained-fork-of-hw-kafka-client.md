---
type: Architecture Decision Record
title: rdkafka-hs is a maintained fork of hw-kafka-client
description: rdkafka-hs continues hw-kafka-client under a new package name with its full git history, keeps the Kafka.* namespace and public names, and restarts its version line at 1.0.0.0.
timestamp: 2026-10-22T00:30:00Z
generated:
  by: claude-code/claude-opus-5-5
  at: "2026-10-22T00:30:00Z"
docId: ADR-3
status: Accepted
date: 2026-10-21
originatingPlan: docs/plans/1-bootstrap-rdkafka-hs-from-the-hw-kafka-client-fork.md
---

# rdkafka-hs is a maintained fork of hw-kafka-client

## Context

`hw-kafka-client`, the Haskell binding to librdkafka maintained at
`haskell-works/hw-kafka-client`, last received a push on 2025-10-03. Its newest
Hackage release, 5.3.0, predates twelve commits on upstream `main`. A fix for an
unobservable consumer fatal error in the default callback poll mode, required by
`mori://shinzui/keiro/okf/adrs/concepts/ADR-11`, was proposed upstream and has had
no response. Several of this author's packages and applications already build
against a commit-pinned fork carrying that fix, and improvement requests written
against the fork cannot be released by anyone.

## Decision

This repository is the maintained successor of the fork. It is published as the
Cabal package `rdkafka-hs` and is identified in `mori.dhall` as a fork whose
upstream is `haskell-works/hw-kafka-client`.

The fork's git history is imported by merging commit
`fe5328a81941e92b93c3f47de14878346c9a5b43` with unrelated histories, so upstream
attribution and `git blame` on inherited code survive. The original MIT license
and copyright notice are retained.

The `Kafka.*` module namespace, public type names and public record field names
of hw-kafka-client 5.3.0 are kept. Dependents migrate by changing the dependency
name and version bounds in each affected Cabal component, not their imports.
The `generic-lens` record conventions of
`mori://shinzui/haskell-jitsurei/docs/core-record-patterns` are not applied to the
existing public records; doing so would break every known dependent for no
behavioural gain and is left to a separate major-version initiative.

The version line restarts at `1.0.0.0`. A new package name has no PVP continuity
with hw-kafka-client, and `CHANGELOG.md` states the lineage (Hackage 5.3.0, the
unreleased upstream commits, and the fatal-error fix) so nothing is hidden.

## Consequences

Existing `hw-kafka-client >=5.3 && <6` constraints exclude `rdkafka-hs-1.0.0.0`
after a name-only replacement; the migration guide and the release validation in
[release validation precedes publication](release-validation-before-publication.md)
account for this. Upstream changes are not merged automatically; anything worth
carrying from `haskell-works/hw-kafka-client` is cherry-picked deliberately.
Publishing to Hackage is outside the first release.
