---
title: "Use prefix matching instead of suffix matching"
type: change
authors: jachris
pr: 3616
---

The operators `drop`, `pseudonymize`, `put`, `extend`, `replace`, `rename` and
`select` were converted from suffix matching to prefix matching and can
therefore address records now.
