# Bundle Update Log

## 2026-10-22
* **Update**: IR-2 AC-5 now targets a tagged release of rdkafka-hs at https://github.com/shinzui/rdkafka-hs, the fork's maintained successor; request identity and origin are unchanged.

## 2026-10-21
* **Addition**: IR-2 requests assign-only consumer construction, partition-attributed per-message errors, and deterministic client destruction for Shōko's bounded Kafka observer.

## 2026-09-30
* **Addition**: IR-1 requests a binding for librdkafka's queue event notification so consumers can wait for records without polling on a timer.
* **Bootstrap**: Establish the improvement-request bundle under the shared coordination.improvementRequests profile.
