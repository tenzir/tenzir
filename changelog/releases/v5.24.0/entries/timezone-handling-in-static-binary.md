---
title: Timezone handling in static binary
type: bugfix
authors:
  - tobim
  - claude
pr: 5649
created: 2026-01-07T16:47:56.531009Z
---

The `format_time` and `parse_time` functions in the static binary now correctly
use the operating system's timezone database.
