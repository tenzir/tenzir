---
title: Fix sigma operator directory handling to load all rules
type: bugfix
authors:
  - mavam
  - claude
pr: 5715
created: 2026-02-03T11:16:25.337105Z
---

The `sigma` operator now correctly loads all rules when given a directory containing multiple Sigma rule files. Previously, only the last processed rule file would be retained because the rules collection was being cleared on every recursive directory traversal.

```tql
sigma "/path/to/sigma/rules"
```

All rules found in the directory and its subdirectories will now be loaded and used to match against input events.
