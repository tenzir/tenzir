---
title: "Enable real-time metrics reporting"
type: change
author: dominiklohmann
created: 2021-02-15T12:24:10Z
pr: 1368
---

All options in `vast.metrics.*` had underscores in their names replaced with
dashes to align with other options. For example, `vast.metrics.file_sink` is now
`vast.metrics.file-sink`. The old options no longer work.
