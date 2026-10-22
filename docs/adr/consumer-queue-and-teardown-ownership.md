---
type: Architecture Decision Record
title: Consumer queue and teardown ownership
description: Manual assignment shares subscription queue routing, and destroyConsumer synchronously stops polling, releases queue references and destroys the client without a graceful group close.
timestamp: 2026-10-22T00:05:10Z
generated:
  by: codex-cli/gpt-6-astra
  at: "2026-10-22T00:05:10Z"
docId: ADR-1
status: Accepted
date: 2026-10-21
---

# Consumer queue and teardown ownership

## Context

The imported consumer routes records to a separate queue in async callback mode.
Its rebalance callback forwards partition queues, but manual `assign` does not.
The background callback loop can therefore consume and discard manually assigned
records. A consumer also owns a client finalizer and a message-queue reference;
explicit destruction must coordinate these resources with that loop.

## Decision

Manual assignment and subscription rebalances must share queue-routing behavior.
EP-5 owns that behavior and consumer lifetime state; EP-4 registers notifications
on the resulting destination queue. Queue acquisition, forwarding and release
must have explicit ownership, with no record-loss interval during assignment.

`closeConsumer` remains the graceful group-close operation. `destroyConsumer`
stops callback polling, releases owned queue references and synchronously runs the
existing client finalizer with `NO_CONSUMER_CLOSE`. It does not first perform a
potentially blocking graceful group close. Callers wanting graceful departure
close before destroying. Concurrent destroy calls join the same cleanup and return
only after it finishes; cleanup is not skipped because a flag was set early.

Applications serialize other operations with teardown and do not destroy from a
librdkafka callback. Event descriptors must stay open until notification has been
disabled or the client has been destroyed. A queue notification indicates a queue
transition, not a promise of a record or notification of every fatal state; async
applications retain bounded fatal-state checks when waiting for records.

## Consequences

Both callback modes need manual-assignment and teardown regression tests. EP-6
must exercise queue notification on an assigned consumer after EP-4 and EP-5 are
combined. The socket test measures elapsed time from entering destroy through
observing EOF, without forcing garbage collection. Graceful close and immediate
destroy remain distinct public operations.

This decision preserves the fatal-error contract in
`mori://shinzui/keiro/okf/adrs/concepts/ADR-11` and implements the request in
`mori://shinzui/hw-kafka-client/okf/improvement-requests/concepts/IR-2`.
EP-1 will adopt this existing record into the profiled ADR bundle and allocate
its stable handle using OKF.
