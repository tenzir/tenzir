---
title: Continued Kafka consumption after malformed Avro messages
type: change
authors:
  - aljazerzen
created: 2026-10-19T10:06:58.353234Z
---

The `from_kafka` operator now skips malformed Avro messages with a warning instead of stopping the pipeline. This keeps a single bad message from blocking later messages:

```tql
from_kafka "events", schema_registry="https://registry.example.com"
```

Warnings identify the topic, partition, and offset. Skipped messages count towards `count`, and their offsets are committed with the batch. Registry requests still retry transient failures; unrecoverable registry, authentication, and schema errors remain terminal without committing the failed batch or later offsets.
