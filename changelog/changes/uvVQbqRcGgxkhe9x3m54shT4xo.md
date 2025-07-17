---
title: "Allow for separating persistent state and log directories"
type: change
authors: dominiklohmann
pr: 758
---

The option `--directory` has been replaced by `--db-directory` and
`log-directory`, which set directories for persistent state and log files
respectively. The default log file path has changed from `vast.db/log` to
`vast.log`.
