---
type: Architecture Decision Record
title: Producer settings merge explicit topic keys after global keys
description: Validate explicit topic properties and merge them into the effective native producer configuration after global properties without replacing its default topic configuration.
timestamp: 2026-10-24T08:17:05Z
generated:
  by: codex-cli/gpt-6.1-sol
  at: "2026-10-24T08:17:05Z"
docId: ADR-12
status: Accepted
date: 2026-10-24
originatingPlan: docs/plans/9-honor-producer-settings-and-release-rejected-callbacks.md
---

# Producer settings merge explicit topic keys after global keys

## Context

The inherited producer retained an explicit topic configuration object while producing by topic name through `produceva`. That producing path used only the native client's configuration, so `sendTimeout`, `topicCompression` and `extraTopicProps` could appear in a retained configuration dump without affecting delivery. Replacing the native default-topic configuration with an empty explicit object would instead lose topic settings supplied through global properties.

## Decision

Validate explicit topic properties through the existing topic-configuration setter, retaining the existing configuration-exception convention and public producer shape. Apply global keys first, then only explicit topic keys to the effective global native configuration before creating the client. Librdkafka accepts topic properties on this configuration. Explicit topic values therefore win over matching global settings, while unrelated global settings survive. Within each property map, the existing right-biased composition remains in force.

Keep the retained `TopicConf` field for public shape and inspection compatibility. It is not evidence that the producing client uses a setting. Do not install a replacement default-topic object to implement this merge.

## Consequences

[EP-9](../plans/9-honor-producer-settings-and-release-rejected-callbacks.md) verifies actual native-client configuration at the `produceva` boundary, timeout expiry through delivery reports, and byte-exact real-broker delivery under all exposed compression choices. Empty topic properties preserve global settings. A configuration dump of the retained topic object alone cannot establish this contract.

This changes no runtime dependency pins, public property types or constructor signatures. Combined coverage and compatibility acceptance remain governed by [ADR-10](release-coverage-requires-behavior-and-ownership-evidence.md).
