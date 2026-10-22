---
okf_version: "0.2"
---

# Files

- [profile.dhall](profile.dhall)

# Architecture Decision Record

- [Consumer fatal errors are surfaced in-band](consumer-fatal-errors-are-surfaced-in-band.md) - Once librdkafka raises a fatal error on a consumer, pollMessage and pollMessageBatch return KafkaResponseError RdKafkaRespErrFatal on every call in both callback poll modes, consumerFatalError returns the cause, and the background loop destroys what it polls.
- [Consumer queue and teardown ownership](consumer-queue-and-teardown-ownership.md) - Manual assignment shares subscription queue routing, and destroyConsumer synchronously stops polling, releases queue references and destroys the client without a graceful group close.
- [Integration tests run against a project-local Redpanda](integration-tests-run-against-a-project-local-redpanda.md) - The integration suite runs only against this project's private Redpanda on Apple Container, behind a manual Cabal flag, and refuses the machine-global broker.
- [rdkafka-hs is a maintained fork of hw-kafka-client](rdkafka-hs-is-a-maintained-fork-of-hw-kafka-client.md) - rdkafka-hs continues hw-kafka-client under a new package name with its full git history, keeps the Kafka.* namespace and public names, and restarts its version line at 1.0.0.0.
- [Release validation precedes publication](release-validation-before-publication.md) - The complete release candidate, including a scratch downstream build with renamed dependency and bounds, is validated before its tag is published, and requests close only after remote tag proof.
- [Toolchain floor is GHC 9.12 and librdkafka 2.15](toolchain-floor-is-ghc-9-12-and-librdkafka-2-15.md) - rdkafka-hs supports only GHC 9.12 and newer with the GHC2024 edition and one shared common stanza, and requires librdkafka 2.15, the version the locked Nix toolchain provides.

