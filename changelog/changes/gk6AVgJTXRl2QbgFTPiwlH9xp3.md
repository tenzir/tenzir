---
title: "Show processes and sockets"
type: change
authors: mavam
pr: 3521
---

The `show` operator now always connects to and runs at a node. Consequently, the
`version` and `nics` aspects moved into operators of their own.
