---
title: Regular expression topic selection for from_kafka
type: feature
authors:
  - mavam
  - codex
created: 2026-06-08T07:08:34.266686Z
---

The `from_kafka` operator can now consume from topics selected by a regular expression when the topic argument starts with `^`:

```tql
from_kafka "^tenant-.*\\.alerts$", offset="beginning"
```

This lets one pipeline consume matching Kafka topics without listing each topic separately.
