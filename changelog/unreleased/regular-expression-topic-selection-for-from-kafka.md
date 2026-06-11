---
title: Regular expression topic selection for from_kafka
type: feature
authors:
  - mavam
  - codex
created: 2026-06-08T07:08:34.266686Z
---

The `from_kafka` operator can now consume from topics selected by a regular
expression when the topic argument starts with `^`:

```tql
from_kafka "^tenant-.*\\.alerts$", offset="beginning"
```

This lets one pipeline consume matching Kafka topics without listing each topic
separately.

As part of this change, `from_kafka` now consumes all topics—exact and
regex—through Kafka's consumer group subscription. This has two visible
effects. First, pipelines that share a `group.id` (default: `tenzir`) now split
the partitions of a topic among themselves instead of each receiving every
message; give each pipeline its own `group.id` to keep full copies. Second,
partition assignments now follow group rebalances, so they may shift when
pipelines with the same `group.id` start or stop. The `exit` option is not
available for regex topics, because a regex subscription has no bounded set of
partitions to reach the end of.
