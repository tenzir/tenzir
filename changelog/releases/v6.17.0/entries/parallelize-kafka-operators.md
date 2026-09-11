---
title: Parallelize the Kafka operators
type: feature
authors:
  - aljazerzen
---

`from_kafka` and `to_kafka` now run as multiple instances when the pipeline is
parallelized.

Both stay sequential where replication would change results: `from_kafka` with
`count`, with an `offset` other than `stored`, or with `group.instance.id` set,
and `to_kafka` with `key`.
