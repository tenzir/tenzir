---
title: "Use prefix matching instead of suffix matching"
type: change
author: jachris
created: 2023-11-15T16:28:32Z
pr: 3616
---

The operators `drop`, `pseudonymize`, `put`, `extend`, `replace`, `rename` and
`select` were converted from suffix matching to prefix matching and can
therefore address records now.
