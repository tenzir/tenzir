---
title: "More parsing functions"
type: bugfix
authors: IyeOnline
pr: 4933
---

Re-defining a predefined grok pattern no longer terminates the application.

The `string.parse_json()` function can now parse single numbers or strings instead
of only objects.

`read_leef` and `parse_leef` now include the `event_class_id` in their output.

`read_yaml` now properly parses numbers as numbers.
