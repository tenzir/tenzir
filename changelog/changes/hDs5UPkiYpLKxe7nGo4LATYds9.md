---
title: "Fix `str` function quotes"
type: bugfix
authors: jachris
pr: 4809
---

The `str` function no longer adds extra quotes when given a string. For example,
`str("") == "\"\""` was changed to `str("") == ""`.
