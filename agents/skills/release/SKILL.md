---
name: release
description: Release rdkafka-hs to Hackage following PVP
argument-hint: "[major|minor|patch]"
disable-model-invocation: true
allowed-tools: Read, Bash, Edit, Glob, Grep, Write, AskUserQuestion
---

# rdkafka-hs Release Skill

Cut a release of the single `rdkafka-hs` package and publish it to
[Hackage](https://hackage.haskell.org/) following the Haskell
[Package Versioning Policy](https://pvp.haskell.org/) (PVP).

This repository is a **single-package** Cabal project (`rdkafka-hs.cabal`,
`cabal.project` with `packages: .`). There is no inter-package dependency
order to manage. The example executable (`kafka-client-example`, behind the
`examples` flag) and the test suites (`rdkafka-hs-test`,
`rdkafka-hs-integration`) ship inside the package tarball; they are **not**
separate Hackage packages.

> **Note on Hackage status.** Before the first run of this skill the project
> was distributed only by git tag (the README's `source-repository-package`
> stanza). This skill publishes `rdkafka-hs` to Hackage and makes Hackage the
> canonical source. The git-tag stanza remains in the README as a fallback.

## Versioning strategy

The PVP version format is `A.B.C.D`:

- `A.B` — **major**: breaking API changes (removed/renamed exports, changed
  types, changed semantics, raised lower bounds on `base`/`librdkafka`/GHC).
- `C` — **minor**: backwards-compatible API additions (new exports, new
  modules, new instances).
- `D` — **patch**: bug fixes, documentation, internal-only changes,
  performance improvements.

Increment rules:

- **major**: increment `B`, reset `C` and `D` to `0` (e.g. `1.0.0.1` → `1.1.0.0`).
- **minor**: increment `C`, reset `D` to `0` (e.g. `1.0.0.1` → `1.0.1.0`).
- **patch**: increment `D` (e.g. `1.0.0.1` → `1.0.0.2`).

## Arguments

`$ARGUMENTS` is optional:

- `major`, `minor`, or `patch` — forces the bump level.
- If omitted, infer the bump level from the changes (see step 2) and confirm
  with the user.

## Steps

### 1. Determine what changed since the last release

- Read the current version from `rdkafka-hs.cabal` (the `version:` field).
- Find the last release tag: `git tag --list 'v*'` (tags are annotated and
  named `v<version>`, e.g. `v1.0.0.1`). Take the highest.
- List commits since that tag: `git log --oneline v<last>..HEAD`.
- If there are no commits since the last tag, tell the user there is nothing
  to release and stop.
- Confirm the working tree is clean (`git status --porcelain`); if not, ask
  the user whether to proceed.

Present a summary: current version, last tag, number of commits since, and a
short categorized digest of those commits.

### 2. Determine the next version using PVP

- If `$ARGUMENTS` is `major`/`minor`/`patch`, use it.
- Otherwise infer from the commits and the `[Unreleased]` changelog section:
  - "breaking", "remove", "rename", changed type/semantics, raised bound → **major**
  - "add", "new", "feature", new export/module → **minor**
  - "fix", "docs", "refactor", internal/perf → **patch**
- Present the proposed bump and the resulting version, and **ask the user to
  confirm** before making any edits.

### 3. Update the version, changelog, and README

#### Cabal version

- Edit `rdkafka-hs.cabal` and set `version:` to the new version.

#### Changelog (`CHANGELOG.md`)

The changelog follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/):
an `## [Unreleased]` section at the top, then dated sections
`## [A.B.C.D] - YYYY-MM-DD` newest-first, with `### Added`, `### Changed`,
`### Fixed`, `### Removed` subsections (only the ones that apply).

- Move the entries currently under `## [Unreleased]` into a new
  `## [<new-version>] - <today>` section (today's date, `YYYY-MM-DD`).
- Leave an empty `## [Unreleased]` section in place at the top.
- If `[Unreleased]` is empty, draft the entries from the commits since the
  last tag, grouped into the Added/Changed/Fixed/Removed subsections.

#### README (`README.md`)

- Update the version bound shown in the Installation and
  "Migrating from hw-kafka-client" sections to match the new version's PVP
  range (e.g. a `1.0.x` release keeps `rdkafka-hs >=1.0 && <1.1`; a `1.1.x`
  release uses `>=1.1 && <1.2`; a `2.x` release uses `>=2 && <3`).
- Update the `tag: v<version>` line in the `source-repository-package`
  fallback stanza to the new tag.
- On the **first** Hackage release, change the Installation lead-in — it
  currently says "rdkafka-hs is not on Hackage; depend on a release tag" — to
  present the Hackage `build-depends` stanza as the primary install path and
  keep the git `source-repository-package` stanza as a fallback.

Show the user **all** diffs (cabal, changelog, README) for review.

### 4. Verify format, build, and checks

Run the project's gates and stop on the first failure:

1. `nix fmt` — format via treefmt-nix (same as `just fmt`).
2. `just test` — the unit suite (`cabal test rdkafka-hs-test`).
3. `just check` — `docs-check` (okf/mori bundle validation) + `bindings-check`
   (the `docs/bindings.md` inventory) + `nix build` + `nix flake check`.

Notes:

- Newly created or edited files must be `git add`-ed before Nix evaluation
  sees them (Nix reads the git tree).
- The integration suite (`just test-it`) needs a project-local Redpanda broker
  on Apple Container and is **not** a hard release gate; run it separately if
  the change touches consumer/producer wire behaviour.
- If any gate fails, fix the issue (or stop and report) before proceeding.

### 5. Commit, tag, and push

- Stage the modified files (`rdkafka-hs.cabal`, `CHANGELOG.md`, `README.md`,
  and any docs touched).
- Commit with the project's Conventional Commits release convention:
  `chore(release): prepare rdkafka-hs <new-version>`. Write a body that
  summarizes what the release contains and why this bump level. Follow any
  footer convention the recent release commits use (e.g. `ExecPlan:` /
  `Intention:` trailers) when applicable.
- Create an **annotated** tag: `git tag -a v<new-version> -m "rdkafka-hs <new-version>"`.
- Push: `git push && git push --tags`.

### 6. Publish to Hackage

From the repository root (the package lives at `.`):

1. `cabal check` — verify there are no packaging issues.
2. `cabal sdist` — build the source tarball
   (`dist-newstyle/sdist/rdkafka-hs-<version>.tar.gz`).
3. `cabal upload --publish dist-newstyle/sdist/rdkafka-hs-<version>.tar.gz` —
   publish the source distribution (**irreversible**).
4. Build and upload Haddock docs:
   - `cabal haddock --haddock-for-hackage --haddock-hyperlink-source --haddock-quickjump`
   - `cabal upload --publish --documentation dist-newstyle/rdkafka-hs-<version>-docs.tar.gz`
5. Report the Hackage URL:
   `https://hackage.haskell.org/package/rdkafka-hs-<version>`.

> **First publication.** Uploading `rdkafka-hs` for the first time registers
> the package name under the maintainer's Hackage account. Make sure
> `~/.config/cabal/config` has valid Hackage credentials (or use
> `cabal upload --username`/`--password`). The library links the C library
> `rdkafka` via `extra-libraries`; Hackage builds docs in its own environment,
> so a local Haddock upload is the reliable way to get rendered docs.

### 7. Create the GitHub release

After the Hackage upload succeeds, create the GitHub release for the tag
(matching the existing release format — title `rdkafka-hs <version>`, body is
the changelog section for this version):

```bash
gh release create v<version> \
  --title "rdkafka-hs <version>" \
  --notes "$(sed -n '/## \[<version>\]/,/## \[/p' CHANGELOG.md | sed '$d')"
```

Append the Hackage link to the notes:
`Hackage: https://hackage.haskell.org/package/rdkafka-hs-<version>`.

Report the GitHub release URL when done.

## Important

- Always ask the user to confirm the **version bump** and the **changelog**
  before committing.
- Never skip the gates: `nix fmt`, `just test`, and `just check` must all pass
  before you commit, tag, or publish.
- The commit and tag are created **only after** the user approves the diffs.
- `cabal upload --publish` is **irreversible** — publish only after the tag is
  pushed and every gate is green.
- If any step fails, **stop and report** the error rather than continuing.
- Do not run this skill automatically (`disable-model-invocation: true`); it is
  operator-invoked.
