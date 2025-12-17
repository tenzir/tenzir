---
title: "Flag for preventing automatic pipeline starts"
type: feature
author: jachris
created: 2025-09-16T16:46:48Z
pr: 5470
---

When the node starts, pipelines that were previously running are immediately
started. The new `--no-autostart` flag can be used to disable this behavior.
