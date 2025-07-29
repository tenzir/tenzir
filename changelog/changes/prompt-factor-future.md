---
title: "Return type of `map` for empty lists"
type: bugfix
authors: jachris
pr: 5385
---

Previously, the `map` function would return the input list when the input was
empty, possibly producing type warnings downstream. It now correctly returns
`list<null>` instead.
