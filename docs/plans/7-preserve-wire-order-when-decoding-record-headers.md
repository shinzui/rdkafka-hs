---
id: 7
slug: preserve-wire-order-when-decoding-record-headers
title: "Preserve wire order when decoding record headers"
kind: exec-plan
created_at: 2026-10-23T23:03:07Z
provenance:
  created_by:
    model: "gpt-6-astra"
    harness: "codex-cli"
    at: 2026-10-23T23:03:07Z
---

# Preserve wire order when decoding record headers


This ExecPlan is a living document. Keep Progress, Surprises & Discoveries, Decision Log, and Outcomes & Retrospective current during implementation. Distill durable project context into `docs/adr/` before completion.


## Purpose / Big Picture


Implement [IR-3](../improvement-requests/3-preserve-wire-order-when-decoding-record-headers.md): consumers must receive Kafka record headers in the same sequence the producer supplied, including repeated names. Headers are name/value byte-string pairs attached to a record; “wire order” means their sequence in the serialized record. Applications selecting the last occurrence of a repeated name must get the last value the producer sent.

For the ordered fixture `event-type=Other, account=two, account=one, event-type=Happened`, both public polling functions must return that exact sequence in both callback modes. Last-occurrence selection must return `Happened` and `one`. Deliver a new immutable tagged release and prove that Shoko's existing real-broker reproduction passes when it fetches that release.


## Progress


- [ ] M1: Exact header order and last-occurrence selection pass broker regressions for both polling APIs and both callback modes, with unchanged public types.
- [ ] M2: The complete release candidate passes repository release checks and Shoko's Kafka spike against that exact commit before publication.
- [ ] M3: The published tag resolves to the validated commit, a fresh downstream checkout passes against the remote release, and IR-3 closes with that evidence.

Creation only: implementation and broker validation have not started.


## Surprises & Discoveries


(None yet.)


## Decision Log


2026-10-23: Correct the shared decoding boundary and preserve the existing list representation. This fixes every consumer conversion path without changing producer order or requiring downstream reversal. Sorting, deduplication, and a library-level last-value policy would lose information and are outside this request.

2026-10-23: Use the established public Git tag plus GitHub release distribution, targeting `1.0.0.1` / `v1.0.0.1` if still available. Remote tag discovery at creation found only `v1.0.0.0`, resolving to `63031a21d070819fe89afbacb6ce76238816748f`. Validate a scratch Shoko checkout before publication and another after publication; do not change the owning downstream checkout. No dependency upgrades or Hackage publication are needed.


## Outcomes & Retrospective


(To be filled during and after implementation.)


## Context and Orientation


`src/Kafka/Internal/Shared.hs`, function `readHeaders`, calls the C library through the foreign function interface (Haskell bindings to native C functions). It reads header indices starting at zero. Its local `go` prepends each pair to an accumulator, then returns that accumulator unchanged on `RdKafkaRespErrNoent`, the native end-of-list result. Thus the decoded list is reversed. Return `Right (reverse acc)` at that successful termination; keep the separate no-headers case and other error handling intact. Prepending followed by one reversal takes time proportional to the number of headers; repeatedly appending to the end would not.

`src/Kafka/Types.hs` defines `Headers` as a wrapper around `[(ByteString, ByteString)]`. `headersFromList` wraps the list and `headersToList` unwraps it. `src/Kafka/Producer.hs`, `withHeaders`, traverses this list in order. `src/Kafka/Consumer/Convert.hs`, `fromMessagePtrAttributed`, obtains headers through `readHeaders`; `fromMessagePtr` delegates to that conversion. In `src/Kafka/Consumer.hs`, `pollMessage` returns `Either KafkaError record`, while `pollMessageAttributed` returns `Either ConsumerMessageError record`, carrying error origin metadata. Both use the same decoder. Callback modes determine whether application polling or a background loop services callbacks; they exercise different native queue paths and both require coverage.

`test-integration/Kafka/Integration/HeadersTest.hs` currently compares `Set.fromList` results, so reversed order passes. Replace this order-insensitive assertion. `test-integration/Main.hs` already registers this module, runs with one test thread, and applies a 60-second timeout per test. `test-integration/Kafka/TestEnv.hs` supplies `withTopic`, `mkConsumerFor`, producer properties, and per-run name prefixes. Its old `receiveMessages` discards errors and only uses `pollMessage`; use a dedicated bounded polling helper for these new assertions. `test-integration/Kafka/Integration/ObserverTest.hs` illustrates explicit callback modes and a monotonic deadline, but its ignore-all-errors retry loop should not be copied into this regression.

Relevant decisions are [ADR-5: private integration broker](../adr/integration-tests-run-against-a-project-local-redpanda.md), [ADR-2: validation before publication](../adr/release-validation-before-publication.md), and [ADR-7: release distribution](../adr/first-release-scope-and-distribution.md). They require the project-owned Redpanda at `127.0.0.1:39092`, names scoped to the run, complete candidate and downstream proof before publication, immutable published tags, and remote retrieval before closing requests. Integration orchestration uses Apple Container on Apple Silicon macOS. The established release checks are `just check` and `just test-it`, with source-distribution validation. `docs/plans/6-cut-the-first-rdkafka-hs-release-and-migration-guide.md` records the preceding release, but this is an independent follow-up, not another child of the completed fork MasterPlan. No existing ADR specifically defines header ordering.

Dependency source was located through Mori: `mori://confluentinc/librdkafka/repos/librdkafka`, repository-relative `src/rdkafka.h` and `src/rdkafka_header.c` (file-level URI pending). `rd_kafka_header_get_all` indexes the header list directly; its documented iterator starts at zero and increments once per successful read. No new native binding is required. Locate dependency sources through Mori, never by inspecting `/nix/store`.

The downstream acceptance owner is [Shoko plan 10](mori://shinzui/shoko/plans/10-implement-the-kafka-topic-evidence-adapter). At creation Mori could locate the project but not yet resolve this plan artifact; retain its canonical URI. Within `mori://shinzui/shoko`, project-relative `shoko-adapters/test/Kafka/Spike.hs` contains the test named `wire header order preserves last duplicate selection`, and `scripts/test-kafka` owns an Apache Kafka broker and runs `kafka-adapter-test`. Those file-level URIs are pending. The existing spike asserts exact duplicate-header order; explicit last-selection assertions belong in this library's new regression. The downstream project-relative `cabal.project` and `flake.module.nix` both pin the old release commit, so scratch validation must account for both. These are project-qualified references with file-level URIs pending.


## Plan of Work


### M1: Preserve order and prove the public contract


Rewrite `test-integration/Kafka/Integration/HeadersTest.hs` around isolated cases under a group named `Headers wire order`. Exercise `pollMessage` and `pollMessageAttributed`, each with `CallbackPollModeSync` and `CallbackPollModeAsync`. Each of these four combinations must test the interleaved duplicate fixture above, multiple distinct names, one header, and no headers. Add an identical repeated pair to a separate fixture so an accidental deduplication cannot pass. Compare the complete lists directly, including counts and positions.

Use `withTopic env suffix 1` with a distinct suffix per case, a fresh consumer group via `makeGroupId`, and `mkConsumerFor topic` with explicit `callbackPollMode`. This covers normal subscription and both queue modes; Shoko provides the assign-only downstream path. Produce a record with the desired `prHeaders` to partition zero, check `produceMessage` for an immediate error, flush, and require the matching record to arrive. Bracket consumer lifetime so assertion failures still close it; `withTopic` owns topic and producer cleanup. Confirm payload, topic, and partition so the assertion cannot inspect an unrelated record.

Use a finite deadline (for example ten seconds with 200 ms polls). Normalize attributed errors with `cmeError` only in the test adapter. Retry poll timeouts; fail other errors immediately with their details. Timeout or missing records must fail, never count as successful empty headers. For last selection, fold left across the returned list, replacing the result whenever the name matches. Assert `Just "Happened"` and `Just "one"` for the duplicate fixture, the correct value for unique names, and `Nothing` for an absent name. This is a consumer assertion, not a new exported selector.

Run the focused group before the decoder change and retain concise evidence that multi-header order fails. Apply the terminal reversal in `readHeaders` and rerun the same group. Update Haddock comments on `Headers`, `headersFromList`, and `headersToList` in `src/Kafka/Types.hs` to promise sequence and duplicate preservation, and add a fix entry to `CHANGELOG.md`. Run the unit suite and complete integration suite. M1 is accepted when the regression fails before the fix, all four combinations pass afterwards, and the complete suites pass.


### M2: Validate the exact release candidate


Prepare `rdkafka-hs.cabal` version `1.0.0.1` and a dated `CHANGELOG.md` release section, retaining the existing unrelated unreleased documentation fix in that release. Check remote tags again before allocating the version; if already taken, choose the next unused patch and update all commands in this plan. Preserve public signatures, dependency bounds, and the existing toolchain floor. Review durable context and add a focused header-order ADR if the public sequence contract is not captured elsewhere. Before writing an ADR, inspect `mori show --full`, type-check `docs/adr/profile.dhall`, allocate its handle with OKF, and maintain the bundle index and log under the existing profile.

Format, build examples, run unit and full broker suites, run `just check`, and build the source distribution. Commit the complete candidate on the current branch with the ExecPlan trailer. Record its full commit SHA (the immutable Git object identifier). Validation may run before the commit only if the committed content is identical; any subsequent source change needs a new candidate and affected checks.

Clone Shoko into a temporary directory from its Mori-located checkout, recording the downstream commit too. In this scratch copy only, change its Cabal source-repository location to the absolute rdkafka-hs repository and its tag to the candidate SHA. Replace only the rdkafka-hs `fetchFromGitHub` source expression in its Nix override with `builtins.fetchGit { url = "file://<absolute-rdkafka-repository>"; rev = "<candidate-SHA>"; }`. This selects committed source rather than a mutable working-tree path. Run `nix develop --command just test-kafka`. Require the wire-header-order test to execute and pass, along with the command's normal broker and non-interference checks. Do not weaken the spike or introduce a caller reversal. M2 is accepted only when all candidate checks and this downstream proof succeed before publishing.


### M3: Publish and verify remote consumption


Publish an annotated tag and GitHub release for the exact accepted candidate, using the changelog section as release notes. Verify the remote peeled tag SHA equals the recorded candidate SHA. Clone the remote release afresh and run its broker suite, demonstrating that the published artifact includes the regressions.

Make a second fresh scratch clone of the same Shoko commit used in M2. Its Cabal source location remains the public HTTPS repository and its tag becomes the released commit SHA. Update the rdkafka-hs Nix override to that same SHA and the correct fetched-source hash; derive the hash with `nix-prefetch-url --unpack`, never copy the previous release's hash. Run the same Kafka command and record the resolved dependency version, SHA, and passing wire-header-order test. This proves remote consumption without local patches. Keep IR-3 open if any of these gates fails.

After success, update `docs/improvement-requests/3-preserve-wire-order-when-decoding-record-headers.md` to `completed` with completion time and a Resolution containing the release tag, SHA, and acceptance evidence. Maintain `docs/improvement-requests/log.md` and validate its profile. Complete this plan's outcomes and ADR distillation. Commit the final evidence separately; do not move the release tag to include later bookkeeping.


## Concrete Steps


Run repository commands from the rdkafka-hs checkout root. Nix supplies the established GHC/librdkafka toolchain; do not upgrade dependencies to implement this fix. The broker commands require the Apple Container service to be available.

```bash
nix develop --command just test-it '--test-options=-p /Headers/'
nix develop --command cabal test rdkafka-hs-test --test-show-details=direct
nix develop --command just test-it
```

The first command must show an ordered-list mismatch before the fix, and the focused group must pass afterwards. Run complete release validation after final candidate edits:

```bash
nix fmt
nix develop --command cabal build all -f examples
nix develop --command cabal test rdkafka-hs-test --test-show-details=direct
nix develop --command just test-it
nix develop --command just check
nix develop --command cabal check
nix develop --command cabal sdist
nix develop
rdkafka_sdist_dir=$(mktemp -d)
tar -xzf dist-newstyle/sdist/rdkafka-hs-1.0.0.1.tar.gz -C "$rdkafka_sdist_dir"
(cd "$rdkafka_sdist_dir/rdkafka-hs-1.0.0.1" && cabal build)
exit
```

Require successful builds and test summaries; investigate new warnings. `just check` includes strict knowledge-bundle validation, binding inventory validation, `nix build`, and flake checks. Stage only intended edits and commit on the current branch, with a Conventional Commit subject and this trailer:

```text
ExecPlan: docs/plans/7-preserve-wire-order-when-decoding-record-headers.md
```

Create the prepublication scratch checkout from the repository root; retain these variables in the same shell, or record their values before starting another shell:

```bash
rdkafka_repo=$(pwd)
rdkafka_candidate=$(git rev-parse HEAD)
shoko_source=$(mori path mori://shinzui/shoko)
rdkafka_proof_dir=$(mktemp -d)
git clone --no-hardlinks "$shoko_source" "$rdkafka_proof_dir/shoko-candidate"
git -C "$rdkafka_proof_dir/shoko-candidate" rev-parse HEAD
```

Apply the scratch-only Cabal and Nix edits described in M2, using `rdkafka_repo` and `rdkafka_candidate`. From the scratch project root run:

```bash
nix develop --command just test-kafka
```

The output must include the actual `wire header order preserves last duplicate selection` test passing and the fixture's accepted real-broker result. A build alone is insufficient. Record the dependency resolution so a cached old package cannot be mistaken for candidate proof.

After all M2 gates pass, save the exact release section into a temporary UTF-8 notes file and publish from the rdkafka-hs root:

```bash
rdkafka_notes=$(mktemp)
python3 - "$rdkafka_notes" <<'NOTES'
from pathlib import Path
import sys
text = Path("CHANGELOG.md").read_text()
section = "## [1.0.0.1]" + text.split("## [1.0.0.1]", 1)[1]
Path(sys.argv[1]).write_text(section.split("\n## [", 1)[0].rstrip() + "\n")
NOTES
git tag -a v1.0.0.1 "$rdkafka_candidate" -F "$rdkafka_notes"
git push origin HEAD
git push origin refs/tags/v1.0.0.1
gh release create v1.0.0.1 --verify-tag --title 'rdkafka-hs 1.0.0.1' --notes-file "$rdkafka_notes"
git ls-remote origin refs/tags/v1.0.0.1 'refs/tags/v1.0.0.1^{}'
gh release view v1.0.0.1 --json tagName,isDraft,isPrerelease,url
```

Expect the peeled tag to equal `rdkafka_candidate`, and both release flags to be false. Verify the published source and downstream independently:

```bash
git clone --branch v1.0.0.1 --depth 1 https://github.com/shinzui/rdkafka-hs.git "$rdkafka_proof_dir/released-client"
(cd "$rdkafka_proof_dir/released-client" && git rev-parse HEAD && nix develop --command just test-it)
git clone --no-hardlinks "$shoko_source" "$rdkafka_proof_dir/shoko-release"
nix-prefetch-url --unpack "https://github.com/shinzui/rdkafka-hs/archive/$rdkafka_candidate.tar.gz"
```

Check out the recorded downstream commit in the second Shoko clone. Apply the M3 pin edits there; the prefetch result can be used as `sha256` in `fetchFromGitHub`. Run `nix develop --command just test-kafka` from that scratch root again. If an ADR or request timestamp changes, use `okf log add --help` for the installed log CLI and maintain the associated bundle log, then finish with:

```bash
nix develop --command just docs-check
git diff --check
```


## Validation and Acceptance


AC-1 requires exact list equality for the duplicate fixture through both polling functions in both callback modes. A set comparison, sorting, a test-only decoder, or only a `headersFromList`/`headersToList` roundtrip cannot establish this. The broker regressions must demonstrably fail against the old decoder and pass with the fix.

AC-2 requires last-occurrence selection to return `Happened` for `event-type`, `one` for `account`, the sole value for a unique name, and no value for an absent name. Empty lists, singleton lists, distinct names, and identical duplicate pairs must survive the real broker path unchanged. Errors and missing records must fail visibly within the test deadline.

AC-3 requires a published immutable release containing source and regressions, passing broker tests on that commit, and passing the named Shoko spike after remote retrieval of that same SHA. Record both the prepublication candidate proof and postpublication remote proof, including the downstream commit tested. A successful local test or unpushed tag does not complete IR-3.


## Idempotence and Recovery


Tests use run-prefixed topics and fresh consumer groups; cleanup must run on failure. `just test-it` starts the private broker idempotently. Do not use the shared loopback broker on port 9092 or purge broker state as a routine retry. Re-run a failed check after addressing its cause; do not convert environment failures into skips.

Scratch checkouts isolate downstream pin edits. Preserve the owning working trees and any unrelated changes. Recreate a scratch checkout if its build provenance is ambiguous. If candidate source changes, record a new SHA and repeat affected validation before publication. Inspect remote tags before retrying a failed push or release command. Published tags are never moved or deleted; a defect discovered after publication requires a new patch release. Post-release evidence belongs in a subsequent documentation commit.


## Interfaces and Dependencies


Keep `Headers`, `headersFromList`, `headersToList`, `pollMessage`, and `pollMessageAttributed` signatures unchanged. The only production behavior change is the order returned by the shared header decoder. The public list preserves repeated names and values; the caller chooses whether and how to select a value. Nullable-header representation and decoder error-policy changes are separate work.

No new Haskell or native dependencies, bindings, toolchain pins, or dependency bounds are required. Use the repository's existing GHC 9.12/librdkafka 2.15 floor, Tasty integration suite, private Redpanda fixture, Nix development shell, and GitHub release tools. Discover any additional dependency APIs through Mori first, and verify authoritative released versions before proposing changed bounds or compatibility workarounds. The downstream acceptance depends on `mori://shinzui/shoko` and its retained broker spike; this plan requires only scratch validation, leaving downstream adoption to its owner.
