---
title: "Fix `str` function quotes"
type: bugfix
author: jachris
created: 2024-11-22T21:45:46Z
pr: 4809
---

The `str` function no longer adds extra quotes when given a string. For example,
`str("") == "\"\""` was changed to `str("") == ""`.
