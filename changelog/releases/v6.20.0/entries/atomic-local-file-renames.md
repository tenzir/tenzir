---
title: Atomic local file renames
type: bugfix
authors:
  - mavam
created: 2026-10-19T19:14:17.874384Z
---

The `from_file` operator now publishes renamed files atomically when the source and destination are on the same local filesystem. Concurrent readers no longer see an empty or partially copied destination. Moves across filesystems still use copy-and-remove.

The `sigma` operator also reads a consistent file revision when a rule file is replaced atomically, preventing spurious reload warnings and truncated rule updates.
