---
type: Architecture Decision Record
title: First release scope and distribution
description: rdkafka-hs 1.0.0.0 is distributed as a git tag of a public GitHub repository, not on Hackage, and deliberately excludes public API renames, admin API expansion, CI, Linux broker orchestration and downstream edits.
timestamp: 2026-10-22T01:37:50Z
generated:
  by: claude-code/claude-opus-5-5
  at: "2026-10-22T01:37:50Z"
docId: ADR-7
status: Accepted
date: 2026-10-22
originatingPlan: docs/plans/6-cut-the-first-rdkafka-hs-release-and-migration-guide.md
---

# First release scope and distribution

## Context

The fork initiative
(`docs/masterplans/1-fork-hw-kafka-client-as-rdkafka-hs.md`) had to ship one
release that downstream packages could pin, carrying the consumer fatal-error
fix and the two improvement requests. Several adjacent pieces of work were
proposed and set aside while planning, and the repository was created private,
which would have required every downstream build to authenticate to fetch it.

## Decision

The first release, `1.0.0.0`, is distributed as the annotated tag `v1.0.0.0` of
the public repository `https://github.com/shinzui/rdkafka-hs`, with a GitHub
release carrying its changelog section. Downstream packages depend on it through a
`source-repository-package` pin over HTTPS and `rdkafka-hs >=1.0 && <1.1`. The
repository was made public on 2026-10-22 so those pins work without
credentials. Published tags are never moved; a defect becomes a new version.

Deliberately excluded from that release, each for its own reason:

- Hackage publication: the package is consumed by pinned downstream projects,
  and a Hackage name and upload policy can be decided separately. `cabal check`
  and a build from `cabal sdist` are still run so an upload stays possible.
- Renaming public record fields or adopting `generic-lens` labels: it would
  break every dependent for no behavioural gain
  ([the fork identity record](rdkafka-hs-is-a-maintained-fork-of-hw-kafka-client.md)).
  The one public field change, `cpCallbackPollMode` becoming a `Maybe`, fixes a
  defect.
- Binding admin operations beyond topic creation and deletion, the mock cluster
  API or interceptors: no consumer needs them yet, and `docs/bindings.md` makes
  the gap visible.
- Continuous integration: the integration suite needs the Apple Container broker,
  and `just check` plus `just test-it` are the release gates.
- Linux broker orchestration for the integration tests
  ([the private-broker record](integration-tests-run-against-a-project-local-redpanda.md)).
- Editing downstream repositories: migration is proved on a throwaway clone of
  `mori://shinzui/kafka-effectful`; each dependent migrates in its own change.

## Consequences

Each exclusion can become its own initiative without reopening this one.
Dependents need `nix` or a GHC 9.12 toolchain with librdkafka 2.15 and pkg-config
to build. A Hackage upload later would need a package-name and version-bound
policy and would not change the tag-based distribution of 1.0.0.0.
