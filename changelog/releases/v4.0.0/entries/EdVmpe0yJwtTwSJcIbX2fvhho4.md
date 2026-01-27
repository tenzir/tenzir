---
title: "Introduce the `tenzir` and `tenzird` binaries"
type: change
author: dominiklohmann
created: 2023-06-02T18:44:18Z
pr: 3187
---

VAST is now called Tenzir. The `tenzir` binary replaces `vast exec` to execute a
pipeline. The `tenzird` binary replaces `vast start` to start a node. The
`tenzirctl` binary continues to offer all functionality that `vast` previously
offered until all commands have been migrated to pipeline operators.
