---
title: "Improved `join` behavior"
type: change
author: jachris
created: 2025-07-18T21:50:27Z
pr: 5356
---

The `join` function now also works with empty lists that are typed as
`list<null>`. Furthermore, it now emits more helpful warnings.
