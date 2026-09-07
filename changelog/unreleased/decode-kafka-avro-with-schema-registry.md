---
title: Decode Kafka Avro with Schema Registry
type: feature
authors:
  - raxyte
created: 2026-09-07T00:00:00Z
---

The `from_kafka` operator now decodes Avro message values directly with
`schema_registry="https://registry.example.com"`. It supports Confluent schema IDs
in payload prefixes and schema GUIDs in Kafka headers, caches writer schemas,
and resolves named schema references. Registry authentication and TLS settings
are independent of Kafka broker settings.

Registry and decoding failures stop consumption without committing the failed
batch or later offsets. Kafka tombstones are skipped and still count towards
`count`.
