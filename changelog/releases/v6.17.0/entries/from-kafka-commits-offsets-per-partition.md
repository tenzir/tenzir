---
title: "`from_kafka` no longer stops committing offsets after one bad partition"
type: bugfix
authors:
  - aljazerzen
---

`from_kafka` committed the offsets of all its partitions in one request and
kept every offset when that request failed. A single partition that could not
be committed — because a rebalance moved it to another consumer — therefore
failed every later commit too, so the operator stopped recording progress for
the whole topic and re-read everything it had already delivered after a
restart.

The operator now reads Kafka's per-partition commit results: partitions that
committed are retired even when another partition in the same request failed,
transient failures are retried on their own, and an offset that this consumer
can never commit is dropped once with a warning. Uncommitted offsets of
partitions that a rebalance takes away are dropped as well, since only their
new owner can commit them.

It also no longer submits an offset for a partition it does not currently own.
Kafka accepts such a commit under the committing member's own generation, so it
overwrote the progress of the consumer that had taken the partition over,
making that consumer re-read or skip records.
