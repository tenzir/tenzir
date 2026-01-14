---
title: "Add spread syntax `...expr` for lists"
type: feature
author: jachris
created: 2024-11-06T17:16:16Z
pr: 4729
---

The spread syntax `...` can now be used inside lists to expand one list into
another. For example, `[1, ...[2, 3]]` evaluates to `[1, 2, 3]`.
