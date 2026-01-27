---
title: "Fix user-defined operators for TQL2"
type: bugfix
author: dominiklohmann
created: 2025-05-05T15:59:57Z
pr: 5169
---

User-defined operators still required the `// tql2` comment at the start or the
`tenzir.tql2` option to be set, despite TQL2 being the default since Tenzir Node
v5.0. They now work as expected.
