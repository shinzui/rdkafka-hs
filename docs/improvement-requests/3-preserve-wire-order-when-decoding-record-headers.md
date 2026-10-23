---
type: Improvement Request
title: Preserve wire order when decoding record headers
description: Return consumed Kafka headers in their original wire order so duplicate header names retain last-value semantics for event type and business-key correlation.
generated:
  by: codex-cli/gpt-6.1-sol
  at: "2026-10-23T22:40:20Z"
timestamp: 2026-10-23T23:36:01Z
requestId: IR-3
status: completed
completedAt: "2026-10-23T23:36:01Z"
resolution: Released in rdkafka-hs v1.0.0.1 (commit 45d9e21ad3c72b56c482173e17ca3fa2bd8b1bc7) at https://github.com/shinzui/rdkafka-hs; a Shoko clone pinned to the release passes its wire-header-order spike.
origin: mori://shinzui/shoko/plans/10-implement-the-kafka-topic-evidence-adapter
acceptanceCriteria:
  - id: AC-1
    statement: Consuming a record preserves the complete wire order of its headers, including repeated names, in headersToList for pollMessage and pollMessageAttributed.
    verification: Produce the ordered headers event-type=Other, account=two, account=one, event-type=Happened to a real broker and assert both public polling APIs return that exact ordered list in synchronous and asynchronous callback modes.
  - id: AC-2
    statement: Last-occurrence selection after decoding returns Happened for event-type and one for account in the AC-1 fixture.
    verification: Select the last matching header from the consumed list and assert the expected event type and correlation value, while unique and absent header cases still pass.
  - id: AC-3
    statement: A published tagged release contains the fix and regression tests so Shoko can consume it without a local patch or header-order workaround.
    verification: The immutable release commit passes its broker integration tests and the Shoko released-client spike's wire-header-order group.
reviews:
  - kind: model
    reviewer: codex-cli/gpt-6.1-sol
    reviewed_at: "2026-10-23T22:40:20Z"
    document_timestamp: "2026-10-23T22:40:20Z"
    scope: technical-accuracy
    outcome: commented
    provider: OpenAI
    model: gpt-6.1-sol
    effort: high
    context: Author self-check against the immutable v1.0.0.0 source and a real Apache Kafka 4.3.1 roundtrip; not an independent review.
---

# Preserve wire order when decoding record headers

## Reproduction and impact

The first broker spike for
[Shoko plan 10](mori://shinzui/shoko/plans/10-implement-the-kafka-topic-evidence-adapter)
found that rdkafka-hs 1.0.0.0 (tag `v1.0.0.0`, commit
`63031a21d070819fe89afbacb6ce76238816748f`) reverses record headers during decoding.
The client otherwise passed assign-only reading, partition-attributed EOF,
read-committed visibility behind an open transaction, aborted-transaction EOF,
and bounded consumer destruction against Apache Kafka 4.3.1/librdkafka 2.15.0.

`src/Kafka/Internal/Shared.hs`, `readHeaders`, reads index 0 upward but prepends
each `(name, value)` to an accumulator and returns the accumulator unchanged at
`RdKafkaRespErrNoent`. Thus `[("event-type", "Happened"), ("header", "value")]`
round-trips as `[("header", "value"), ("event-type", "Happened")]`.
`src/Kafka/Producer.hs`, `withHeaders`, sends the supplied list in order.

Kafka allows repeated header names. Shoko must select the last wire occurrence
for event type and business-key correlation. Reversing the list silently selects
the first wire occurrence instead, which can assign a record to the wrong query.
Shoko's plan explicitly gates its client-backed adapter on a passing released-client
spike and forbids local client patches or workarounds for a failing client contract.

The retained reproduction is the `Kafka.Spike` module in
`mori://shinzui/shoko` (project-relative
`shoko-adapters/test/Kafka/Spike.hs`, artifact-level URI pending), group
`wire header order preserves last duplicate selection`.
Run `nix develop --command just test-kafka` in that project. The fixture owns its
broker and cleans it up on failure. The ordered duplicate-header assertion fails
on 1.0.0.0; no Shoko client binding has been promoted.

## Requested change

Preserve wire order at the library decoding boundary and add direct broker
roundtrip tests for repeated headers in both public polling APIs and callback
modes. Publish a new release carrying the regression checks. Do not change the
order accepted by producer `headersFromList` or ask callers to reverse lists.

## Resolution

Completed on 2026-10-23 by the release `v1.0.0.1` of
`https://github.com/shinzui/rdkafka-hs` (annotated tag object
`33e2ab5f31be57e14c9820d4ad99032d3e9b41f1`, commit
`45d9e21ad3c72b56c482173e17ca3fa2bd8b1bc7`), implemented by
[plan 7](../plans/7-preserve-wire-order-when-decoding-record-headers.md).
`readHeaders` now reverses its accumulator once at the end of the native header
list, so every consumer path returns headers in wire order; the `Headers` API is
unchanged. [ADR-9](../adr/consumed-headers-preserve-wire-order.md) records the
ordering contract.

AC-1 and AC-2: `test-integration/Kafka/Integration/HeadersTest.hs` produces the
fixture `event-type=Other, account=two, account=one, event-type=Happened`, plus
distinct-name, identical-pair, single and empty fixtures, to the private
Redpanda and compares the complete decoded list through `pollMessage` and
`pollMessageAttributed` in both callback poll modes. Last-occurrence selection
returns `Happened` and `one`, the sole value for unique names, and nothing for an
absent name. Against the 1.0.0.0 decoder 12 of the 20 cases failed (every
multi-pair fixture in all four combinations); with the fix all 20 pass.

AC-3: a fresh HTTPS clone of `v1.0.0.1` resolves to the commit above and passes
all 116 broker integration tests. A clone of
`mori://shinzui/shoko` at commit `09204ad4d0894fda10220c68b81474b93ae720ce`,
with only its `cabal.project` tag, `cabal.project.freeze` version and
`flake.module.nix` `fetchFromGitHub` revision and hash moved to the release,
fetched the source from GitHub, resolved `rdkafka-hs-1.0.0.1` and passed
`nix develop --command just test-kafka` against Apache Kafka 4.3.1, including
`wire header order preserves last duplicate selection` and its non-interference
checks (37 of 37 tests). No caller reversal or local patch is involved.
