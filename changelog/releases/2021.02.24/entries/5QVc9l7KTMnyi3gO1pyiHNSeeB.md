---
title: "PRs 1330-1376"
type: feature
author: dominiklohmann
created: 2021-02-03T08:51:44Z
pr: 1330
---

The meta index now stores partition synopses in separate files. This will
decrease restart times for systems with large databases, slow disks and
aggressive `readahead` settings. A new config setting `vast.meta-index-dir`
allows storing the meta index information in a separate directory.
