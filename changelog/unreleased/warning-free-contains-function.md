---
title: Warning-free contains function
type: bugfix
authors:
  - tobim
created: 2026-10-06T09:27:18.370647Z
---

The `contains(value, "needle")` function and `value.contains("needle")` method now run without deprecation warnings. Both retain the same behavior and options as `search`.
