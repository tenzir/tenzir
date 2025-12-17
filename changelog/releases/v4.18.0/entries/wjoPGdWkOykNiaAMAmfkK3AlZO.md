---
title: "Simplify the node actor setup"
type: change
author: dominiklohmann
created: 2024-07-05T13:56:37Z
pr: 4343
---

The deprecated `vast` symlink for the `tenzir-ctl` binary that offeres backwards
compatiblity with versions older than Tenzir v4—when it was called VAST—no
longer exists.

The deprecated `tenzir.db-directory` option no longer exists. Use
`tenzir.state-directory` instead.
