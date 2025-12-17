---
title: "Remove many deprecated things"
type: change
author: dominiklohmann
created: 2024-04-07T18:03:24Z
pr: 4103
---

The `tenzir-ctl count <expr>` command no longer exists. It has long been
deprecated and superseded by pipelines of the form `export | where <expr> |
summarize count(.)`.

The deprecated `tenzir-ctl status` command and the corresponding `/status`
endpoint no longer exist. They have been superseded by the `show` and `metrics`
operators that provide more detailed insight.

The deprecated `tenzir.aging-frequency` and `tenzir.aging-query` options no
longer exist. We recommend using the compaction or disk monitor mechanisms
instead to delete persisted events.
