---
title: "Implement `and` and `or` for `null_type`"
type: bugfix
author: raxyte
created: 2024-10-23T10:40:32Z
pr: 4689
---

The boolean operators `and`/`or` now work correctly for the type `null`.
Previously, `null and false` evaluated to `null`, and a warning was emitted.
Now, it evaluates to `false` without a warning.
