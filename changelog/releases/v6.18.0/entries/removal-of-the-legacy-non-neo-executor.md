---
title: Removal of the legacy non-neo executor
type: change
authors:
  - aljazerzen
created: 2026-09-10T11:29:52.813582Z
---

Tenzir now runs all pipelines with the current executor. The legacy non-neo executor and its compatibility-only operator implementations have been removed.

Pipelines using supported operators continue to run without changes. Operators that were available only through the legacy executor must be migrated to their current equivalents before upgrading.
