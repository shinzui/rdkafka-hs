---
okf_version: "0.2"
---

# Files

- [profile.dhall](profile.dhall)

# Architecture Decision Record

- [Consumer queue and teardown ownership](consumer-queue-and-teardown-ownership.md) - Manual assignment shares subscription queue routing, and destroyConsumer synchronously stops polling, releases queue references and destroys the client without a graceful group close.
- [rdkafka-hs is a maintained fork of hw-kafka-client](rdkafka-hs-is-a-maintained-fork-of-hw-kafka-client.md) - rdkafka-hs continues hw-kafka-client under a new package name with its full git history, keeps the Kafka.* namespace and public names, and restarts its version line at 1.0.0.0.
- [Release validation precedes publication](release-validation-before-publication.md) - The complete release candidate, including a scratch downstream build with renamed dependency and bounds, is validated before its tag is published, and requests close only after remote tag proof.
- [Toolchain floor is GHC 9.12 and librdkafka 2.15](toolchain-floor-is-ghc-9-12-and-librdkafka-2-15.md) - rdkafka-hs supports only GHC 9.12 and newer with the GHC2024 edition and one shared common stanza, and requires librdkafka 2.15, the version the locked Nix toolchain provides.

