# Bundle Update Log

## 2026-10-24
* **created**: Scope native metadata results and preserve topic partition and group decoding errors
* **created**: Require threaded runtime for async consumers and verify synchronous fallback choice
* **created**: Scope diagnostic streams and native dump arrays while preserving caller handles
* **updated**: Apply explicit-key topic configuration merging to consumers
* **created**: Preserve member failures and partial success in consumer partition operations
* **created**: Check native rebalance failures and scope cooperative error ownership
* **created**: Scope every polled native message and surface header decoding failures.
* **created**: Apply user-selected last-value-wins subscription property composition.
* **created**: Scope transaction error decoding and temporary offset metadata ownership.
* **updated**: Accept callback ownership evidence with semantic mutations and native diagnostic controls.
* **updated**: Link callback owner ordering and scoped native child cleanup.
* **created**: Record persistent callback ownership and native shutdown ordering.
* **created**: Record ordered seek member errors, initiation-only timeout semantics and rewind propagation.
* **Clarification**: Decide enqueue ownership from the nullable native error pointer and free rejected roots before decoding; preserve native error-object codes.
* **Decision**: Accept conditional per-message callback ownership transfer and containment of delivery exceptions within Haskell threads.
* **Decision**: Accept merging explicit producer topic settings after global settings with actual-client configuration and delivery evidence.
* **Update**: ADR-11 records scoped admin ownership, borrowed result lifetimes, configured request deadlines and C-representation validation.
* **Update**: Record accepted fixture and coverage infrastructure, including source mapping and explicit zero coverage for unlinked modules; repair/API/environment obligations remain open.
* **Decision**: Require public-contract, native-ownership, and supported-environment evidence for release coverage; implementation is planned in MasterPlan 2.

## 2026-10-23
* **Addition**: ADR-9 records that consumed headers preserve wire order, repeated names and identical pairs included, and that value selection is the caller's choice.

## 2026-10-22
* **Amendment**: ADR-4: enforce the librdkafka floor with a build-time header check instead of pkgconfig-depends, which fails in downstream dev shells; ADR-8 notes that the dev shell must match downstream ones.
* **Addition**: ADR-7 records the first release's tag-based public distribution and deliberate exclusions; ADR-8 records that the Nix scaffold is seihou-managed and how to reconfigure it.
* **Update**: ADR-1: record the implemented queue routing for manual assignment, the ClientLifecycle teardown state and destroyConsumer, and why a second destroy path was rejected.
* **Amendment**: ADR-4: the librdkafka floor is enforced with pkgconfig-depends, and newer releases are adopted by moving the Nix pin and regenerating the binding inventory.
* **Addition**: ADR-5 runs integration tests only against the project-local Redpanda behind the integration flag; ADR-6 records that consumer fatal errors are surfaced in-band in both poll modes.
* **Addition**: ADR-3 records rdkafka-hs as the maintained fork of hw-kafka-client; ADR-4 records the GHC 9.12 and librdkafka 2.15 toolchain floor.
* **Migration**: Adopt the shared architecture-decision profile; ADR-1 and ADR-2 are the existing review records.
