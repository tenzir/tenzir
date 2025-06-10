---
title: "Introduce the `tenzir` and `tenzird` binaries"
type: change
authors: dominiklohmann
pr: 3187
---

VAST is now called Tenzir. The `tenzir` binary replaces `vast exec` to execute a
pipeline. The `tenzird` binary replaces `vast start` to start a node. The
`tenzirctl` binary continues to offer all functionality that `vast` previously
offered until all commands have been migrated to pipeline operators.
