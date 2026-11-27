---
title: Kafka consumers stay in their group under backpressure
type: bugfix
authors:
  - zedoraps
created: 2026-11-03T18:01:42.367329Z
---

The `from_kafka` operator now stays in its consumer group when downstream
operators block for longer than `max.poll.interval.ms`. Previously, the
consumer left the group after the poll interval, offset commits failed with
`Unknown member`, and the pipeline stopped with `unexpected kafka error`.

When the operator gets partitions back after a rebalance, it no longer commits
offsets of records it read before the rebalance. Those offsets could move the
group's position behind the progress of another consumer. Records whose offsets
are skipped are delivered again, in line with at-least-once delivery.

If the poll interval is still exceeded, the error now says that the consumer
left its group because it did not poll within `max.poll.interval.ms`.
