---
title: Warning-free scoped builtin operators
type: bugfix
authors:
  - tobim
created: 2026-10-06T09:12:16.579342Z
---

Builtin operators with `::` names no longer emit deprecation warnings. For example, both `context::lookup "ctx", key=src_ip` and `context_lookup "ctx", key=src_ip` work without a warning.
