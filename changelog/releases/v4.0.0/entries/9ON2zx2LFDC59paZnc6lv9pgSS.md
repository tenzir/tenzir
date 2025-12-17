---
title: "Fix rare crash when transforming sliced nested arrays"
type: bugfix
author: dominiklohmann
created: 2023-05-25T15:08:12Z
pr: 3171
---

Using transformation operators like `summarize`, `sort`, `put`, `extend`, or
`replace` no longer sometimes crashes after a preceding `head` or `tail`
operator when referencing a nested field.

The `tail` operator sometimes returned more events than specified. This no
longer happens.
