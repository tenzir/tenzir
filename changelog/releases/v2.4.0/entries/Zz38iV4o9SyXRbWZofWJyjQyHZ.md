---
title: "Emit metrics from the filesystem actor"
type: feature
author: dominiklohmann
created: 2022-09-14T10:52:45Z
pr: 2572
---

VAST now emits metrics for filesystem access under the keys
`posix-filesystem.{checks,writes,reads,mmaps,erases,moves}.{successful,failed,bytes}`.
