---
type: Architecture Decision Record
title: Toolchain floor is GHC 9.12 and librdkafka 2.15
description: rdkafka-hs supports only GHC 9.12 and newer with the GHC2024 edition and one shared common stanza, and requires librdkafka 2.15, the version the locked Nix toolchain provides.
timestamp: 2026-10-22T00:51:05Z
generated:
  by: claude-code/claude-opus-5-5
  at: "2026-10-22T00:30:00Z"
docId: ADR-4
status: Accepted
date: 2026-10-21
originatingPlan: docs/plans/1-bootstrap-rdkafka-hs-from-the-hw-kafka-client-fork.md
---

# Toolchain floor is GHC 9.12 and librdkafka 2.15

## Context

hw-kafka-client 5.3.0 targeted GHC 8.4 through 9.10 with `Haskell2010` and
per-component warning flags. This author's projects follow
`mori://shinzui/haskell-jitsurei/docs/core-standards`, which requires GHC 9.12 or
newer, `GHC2024`, a shared set of default extensions and explicit deriving
strategies. The project is built by the seihou-managed `nix-haskell-flake`
module, whose pinned `haskell-nix-dev` toolchain provides GHC 9.12.4 as the
primary compiler and whose locked nixpkgs provides librdkafka 2.15.0 and c2hs
0.28.8.

## Decision

Only GHC 9.12 and newer are supported. `rdkafka-hs.cabal` declares
`tested-with: ghc ==9.12.4` and `base >=4.21`. A secondary compiler is added to
`tested-with` only after the project wires a shell for it and its build and unit
tests pass there.

Every Cabal component imports one `common common-options` stanza copied from the
`haskell-library` template: `default-language: GHC2024`; `DeriveAnyClass`,
`DuplicateRecordFields`, `OverloadedLabels` and `OverloadedStrings` as default
extensions; and the warning set `-Wall -Wcompat -Widentities
-Wincomplete-uni-patterns -Wincomplete-record-updates -Wredundant-constraints
-fhide-source-paths -Wmissing-export-lists -Wpartial-fields
-Wmissing-deriving-strategies`. Every hand-written `deriving` clause names its
strategy. Imports use postpositive `qualified`. Warnings that only c2hs-generated
code in `src/Kafka/Internal/RdKafka.chs` produces are disabled in that file alone,
with a comment.

The librdkafka floor is 2.15, the version the locked nixpkgs provides, not the
newest upstream release. The binding is compiled and tested there first; newer
headers are additive.

The floor is enforced, not only documented: the library declares
`pkgconfig-depends: rdkafka >=2.15`, so Cabal's solver rejects an older
librdkafka at configure time, and `Kafka.Version` reports the library actually
linked at run time. A newer librdkafka release is adopted by moving the Nix pin
(through a new `nix-haskell-flake` release), raising this bound only if the
binding comes to need it, and regenerating `docs/bindings.md` from that release's
header with `just bindings-inventory`, which shows what the release added.
(Amended 2026-10-22 by
[the binding refresh plan](../plans/3-refresh-the-librdkafka-bindings-for-2-15-and-newer.md).)

## Consequences

`DeriveAnyClass` combined with strategy-less deriving would silently derive empty
instances on newtypes (for example `Num`, `IsString`, `Semigroup`), so explicit
strategies are a correctness requirement here, not only style. Anyone building
outside Nix must supply GHC 9.12 or newer, c2hs and librdkafka 2.15 headers
with pkg-config files for librdkafka and the libraries its `rdkafka.pc` lists
under `Requires.private`; the dev shell adds those through `flake.module.nix`.
Raising either floor is a deliberate decision recorded by amending this record.
