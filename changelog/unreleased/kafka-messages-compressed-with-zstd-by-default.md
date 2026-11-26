---
title: Kafka messages compressed with zstd by default
type: change
authors:
  - jachris
created: 2026-11-26T11:04:37.231634Z
---

The `to_kafka` operator now compresses messages with zstd by default, which reduces the bytes sent over the network. Kafka consumers decompress messages transparently, so no changes are needed on the receiving side. Brokers must run Kafka 2.1 or later to accept zstd-compressed messages.

To keep the previous behavior, set `compression.type` explicitly, for example `options={"compression.type": "none"}`, or pick another codec such as `lz4`. A `compression.type` or `compression.codec` in `options` or in `kafka.yaml` takes precedence over the default. The bytes-written metrics continue to count the payload before compression.
