---
title: Fixed default compaction rules for metrics and diagnostics
type: bugfix
authors:
  - jachris
pr: 5629
created: 2025-12-23T07:34:38.323417Z
---

The default compaction rules for `tenzir.metrics.*` and `tenzir.diagnostic` events now correctly use the `timestamp` field instead of import time.

Previously, these built-in compaction rules relied on import time to determine which events to compact, which could lead to inconsistent results as the import time is not computed per-event. As a result, it was possible that metrics and diagnostics were not deleted even though they expired.
