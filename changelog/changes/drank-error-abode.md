---
title: "Flag for preventing automatic pipeline starts"
type: feature
authors: jachris
pr: 5470
---

When the node starts, pipelines that were previously running are immediately
started. The new `--no-autostart` flag can be used to disable this behavior.
