---
title: Faster event branching
type: change
authors:
  - aljazerzen
  - claude
created: 2026-08-27T07:56:04.737561Z
---

Pipelines that branch events with `if` now spend less time copying them — up to
9x more throughput on a batch whose matching events are contiguous.

When the condition holds for a few contiguous stretches of a batch, the branches
are handed views of that batch instead of freshly built copies. This is the
common case for conditions over a sorted field, a time window, or a schema
discriminator:

```tql
if severity >= 4 {
  to_splunk "https://splunk.example.com:8088"
} else {
  to_hive "s3://bucket/archive"
}
```

A batch whose matching events fall into at most four runs is now split without
copying. Beyond four the previous behavior applies, because each view becomes a
message of its own and past that point the messages cost more than the copy
saves.

Fragmented conditions, which still have to copy, gain about 20% from a faster
scan over the condition.

Keyed and round-robin fan-out to parallel workers no longer does per-worker
bookkeeping that it went on to discard when the routing key was already grouped.
