---
title: Failed Kafka message delivery no longer passes silently
type: bugfix
authors:
  - jachris
  - claude
prs:
  - 6406
created: 2026-07-02T10:37:45.123194Z
---

The `to_kafka` operator now stops the pipeline with an error when the broker fails to accept a produced message, instead of silently dropping the event and reporting success.

librdkafka reports some delivery failures only after a message has been queued for sending and its retries have been exhausted, and these previously went unnoticed. Examples include a delivery that times out because the broker is unreachable, a message that exceeds the topic's maximum size, and a write rejected because the client is not authorized for the topic. Each such failure now produces a diagnostic that identifies the topic and the reason.
