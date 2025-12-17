---
title: "Allow for separating persistent state and log directories"
type: change
author: dominiklohmann
created: 2020-02-22T07:21:37Z
pr: 758
---

The option `--directory` has been replaced by `--db-directory` and
`log-directory`, which set directories for persistent state and log files
respectively. The default log file path has changed from `vast.db/log` to
`vast.log`.
