---
title: "Fix using `summarize \u2026 by x` when `x` is of type `null`"
type: bugfix
author: dominiklohmann
created: 2024-06-12T07:05:15Z
pr: 4289
---

The `summarize` operator no longer crashes when grpuping by a field of type
`null`, i.e., a field whose type could not be inferred because all of its values
were `null`.
