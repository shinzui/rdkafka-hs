# Bundle Update Log

## 2026-10-23
* **Completion**: IR-3 completed by rdkafka-hs v1.0.0.1 (commit 45d9e21ad3c72b56c482173e17ca3fa2bd8b1bc7); a Shoko clone pinned to the release passes its wire-header-order spike.
* **Addition**: IR-3 requests preserving consumed header wire order; the Shoko released-client spike found reversed duplicate-header semantics in v1.0.0.0.

## 2026-10-22
* **Completion**: IR-1 and IR-2 completed by rdkafka-hs v1.0.0.0 (commit 63031a21d070819fe89afbacb6ce76238816748f); a kafka-effectful clone pinned to the tag builds and passes its tests.
* **Update**: IR-2: implemented in rdkafka-hs by newAssignedConsumer, pollMessageAttributed and destroyConsumer, with integration evidence for AC-1 to AC-4; AC-5 awaits the first tagged release.
* **Update**: IR-1: implemented in rdkafka-hs by setConsumerQueueIOEvent with integration evidence for AC-1 and AC-2; AC-3 awaits the first tagged release.
* **Update**: IR-2 AC-5 now targets a tagged release of rdkafka-hs at https://github.com/shinzui/rdkafka-hs, the fork's maintained successor; request identity and origin are unchanged.

## 2026-10-21
* **Addition**: IR-2 requests assign-only consumer construction, partition-attributed per-message errors, and deterministic client destruction for Shōko's bounded Kafka observer.

## 2026-09-30
* **Addition**: IR-1 requests a binding for librdkafka's queue event notification so consumers can wait for records without polling on a timer.
* **Bootstrap**: Establish the improvement-request bundle under the shared coordination.improvementRequests profile.
