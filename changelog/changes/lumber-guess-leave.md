---
title: "Deprecation of `split_at_null` option of `read_lines`"
type: change
authors: jachris
pr: 5431
---

The `split_at_null` option of the `read_lines` operator is now deprecated.
Instead, you can use `read_delimited "\0"`.
