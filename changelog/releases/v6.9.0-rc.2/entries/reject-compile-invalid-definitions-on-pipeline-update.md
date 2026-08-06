---
title: Reject compile-invalid definitions on pipeline update
type: bugfix
authors:
  - gitryder
  - claude
created: 2026-07-29T08:35:47.830081Z
---

Updating a pipeline to a definition that parses but does not compile (for
example, one referencing an unknown operator, producing byte output, or not
ending in a sink) no longer silently replaces the previous working definition.
The update is now rejected with the compile diagnostics, matching the validation
that already happens in the Explorer.
