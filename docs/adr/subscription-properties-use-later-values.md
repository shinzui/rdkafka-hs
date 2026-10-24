---
type: Architecture Decision Record
title: Subscription properties use later values
description: Compose subscription topic sets by union and property maps with later values taking precedence, consistently with consumer and producer properties.
timestamp: 2026-10-24T14:47:46Z
generated:
  by: codex-cli/gpt-6.1-sol
  at: "2026-10-24T14:47:46Z"
docId: ADR-17
status: Accepted
date: 2026-10-24
originatingPlan: docs/plans/14-cover-the-public-api-and-pure-data-laws.md
---

# Subscription properties use later values

## Context

Consumer and producer property composition prefers later explicit values. Subscription composition accumulated topic names but preferred earlier property values. Consequently, `offsetReset Earliest <> offsetReset Latest` retained earliest, contrary to the composition rule expected by EP-14's generated model.

## Decision

The user explicitly selected later subscription values over preserving the inherited precedence. Compose subscription topic sets by union and property maps with the right operand taking precedence. Identity and associativity remain required. The last explicit offset-reset policy wins, including conflicts between the dedicated helper and extra subscription properties.

## Consequences

This intentionally changes the inherited subscription precedence. The changelog records it. Generated programs check an independent last-write model; a minimized inherited failure is `[Reset Earliest, Reset Latest]`. Fixed examples and real consumers in both callback modes verify that the last earliest reset reads a record produced before subscription. Topic deduplication and accumulation remain set operations.
