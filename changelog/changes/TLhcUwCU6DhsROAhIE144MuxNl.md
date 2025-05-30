---
title: "Restrict log file creation to 'vast start'"
type: change
authors: lava
pr: 803
---

The log folder `vast.log/` in the current directory will not be created by
 default any more. Users must explicitly set the `system.file-verbosity` option
 if they wish to keep the old behavior.
