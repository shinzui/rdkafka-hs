---
type: Architecture Decision Record
title: Native clients own persistent Haskell callback bridges
description: Transfer persistent callback registrations to a client-independent owner released after native shutdown, with scoped cleanup for failed construction and native child handles.
timestamp: 2026-10-24T12:29:36Z
generated:
  by: codex-cli/gpt-6.1-sol
  at: "2026-10-24T12:29:36Z"
docId: ADR-15
status: Accepted
date: 2026-10-24
originatingPlan: docs/plans/11-own-native-callbacks-through-client-teardown.md
---

# Native clients own persistent Haskell callback bridges

## Context

A Haskell function pointer roots its callback closure until explicitly freed. Native configuration copies share these addresses. Finalizing only the configuration cannot release callbacks that themselves capture configuration, and freeing registrations before native shutdown leaves callable invalid addresses.

## Decision

An internal owner records every allocated bridge while Building, becomes Attached to one native client, and becomes Released after native destruction. Its data and the native finalizer never capture the owning client ForeignPtr. Replaced registrations remain owned until teardown. Each completed release is recorded before releasing the next pointer, so retry does not repeat completed work.

Mask allocation through registration and native installation of a callback address. Construction failures release unattached owners; a failed native creation also destroys its caller-owned configuration duplicate. After transfer, the native client's existing Haskell finalizer is the sole registration releaser and releases bridges only after native shutdown returns. Original configurations remain passive inspection views and cannot install callbacks or construct another client through an attached/released owner.

Consumer graceful close and producer flush/close retain their existing semantics. Explicit consumer destruction serializes through the lifecycle lock; concurrent callers wait for completed cleanup. Appropriately closed clients also release registrations through GC. Callback-supplied consumer wrappers borrow the native pointer and do not acquire its deallocator; retain the owning consumer for subsequent operations.

Native child handles whose destruction can invoke callbacks use Haskell finalizers or safe scoped destruction. Public operations retain the client through explicit child cleanup, including acquisition and exception paths. A reference between GC finalizers alone does not guarantee completion ordering.

Exceptions from foreign callbacks are contained and reported through the runtime's uncaught-exception handler on a fresh unmasked Haskell thread. Rebalance user actions are contained individually so mandatory native assignment/revocation still runs. Per-message StablePtrs remain separately owned by [ADR-13](delivery-callbacks-transfer-once-and-keep-exceptions-in-haskell.md).

## Consequences

[EP-11](../plans/11-own-native-callbacks-through-client-teardown.md) records the implementation and remaining acceptance work. Its probes cover live callbacks, retained destroyed wrappers, configuration captures, replacement, construction failure, GC, concurrent teardown and cancellation at observed native stages. Native diagnostics, mutation evidence and final combined acceptance remain required before that child is Complete.
