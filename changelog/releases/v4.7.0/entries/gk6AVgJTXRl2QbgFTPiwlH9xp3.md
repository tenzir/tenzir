---
title: "Show processes and sockets"
type: change
author: mavam
created: 2023-12-12T12:32:18Z
pr: 3521
---

The `show` operator now always connects to and runs at a node. Consequently, the
`version` and `nics` aspects moved into operators of their own.
