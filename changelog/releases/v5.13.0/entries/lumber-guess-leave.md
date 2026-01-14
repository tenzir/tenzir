---
title: "Deprecation of `split_at_null` option of `read_lines`"
type: change
author: jachris
created: 2025-08-20T14:09:25Z
pr: 5431
---

The `split_at_null` option of the `read_lines` operator is now deprecated.
Use `read_delimited "\0"` instead.
