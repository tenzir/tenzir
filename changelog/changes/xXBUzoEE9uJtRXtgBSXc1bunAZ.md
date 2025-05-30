---
title: "Fix using `summarize … by x` when `x` is of type `null`"
type: bugfix
authors: dominiklohmann
pr: 4289
---

The `summarize` operator no longer crashes when grpuping by a field of type
`null`, i.e., a field whose type could not be inferred because all of its values
were `null`.
