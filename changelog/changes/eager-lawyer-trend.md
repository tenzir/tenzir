---
title: "Improved `join` behavior"
type: change
authors: jachris
pr: 5356
---

The `join` function now also works with empty lists that are typed as
`list<null>`. Furthermore, it now emits more helpful warnings.
