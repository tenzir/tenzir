---
title: OCSF 1.8.0 support in ocsf::derive
type: change
authors:
  - mavam
  - codex
pr: 5939
created: 2026-03-23T16:33:57.938809Z
---

The `ocsf::derive` operator now supports OCSF `1.8.0` events.

For example, you can now derive enum and sibling fields for events that declare
`metadata.version: "1.8.0"`:

```tql
from {metadata: {version: "1.8.0"}, class_uid: 1007}
ocsf::derive
```

This keeps OCSF normalization pipelines working when producers emit `1.8.0`
events.
