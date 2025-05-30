---
title: "Emit metrics from the filesystem actor"
type: feature
authors: dominiklohmann
pr: 2572
---

VAST now emits metrics for filesystem access under the keys
`posix-filesystem.{checks,writes,reads,mmaps,erases,moves}.{successful,failed,bytes}`.
