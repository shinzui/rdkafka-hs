---
type: Architecture Decision Record
title: Configuration strings reject embedded NUL
description: Reject embedded NUL in shared configuration setters before native marshalling while preserving ordinary native validation and empty-value semantics.
timestamp: 2026-10-24T18:45:17Z
generated:
  by: codex-cli/gpt-6.1-sol
  at: "2026-10-24T18:45:17Z"
docId: ADR-24
status: Accepted
date: 2026-10-24
originatingPlan: docs/plans/14-cover-the-public-api-and-pure-data-laws.md
---

# Configuration strings reject embedded NUL

## Context

The shared producer/consumer setters marshal Haskell text into NUL-terminated C strings. Embedded NUL in either a property key or value silently applies only its prefix. The inherited implementation accepts all twelve global, topic and subscription regression cases, including both consumer callback modes.

## Decision

Validate keys and values in both shared setters before calling native code. Throw `KafkaInvalidConfigurationValue` with an explicit NUL diagnostic, following the existing setter exception convention. Keep ordinary invalid/unknown properties delegated to native validation. Do not include the full configuration value in this diagnostic.

Empty values retain native semantics. In particular, librdkafka normalizes an empty value to an unset value, so an empty client ID selects its native default. Ordinary Unicode client IDs retain their exact configuration dump value.

## Consequences

Producer global/topic properties and consumer global/subscription properties share the same representation rule. Tests verify rejection rather than accidentally accepting a prefix and verify native empty/unknown-key behavior separately. See [EP-14](../plans/14-cover-the-public-api-and-pure-data-laws.md). This rule concerns configuration setters; other public string arguments retain their own contract obligations.

The dependency source is `mori://confluentinc/librdkafka`, project-relative `src/rdkafka_conf.c`; its artifact-level source URI is pending. The native setter normalizes empty strings before validation.
