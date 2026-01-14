---
title: "Fix partition selection for the `rebuild` command"
type: bugfix
author: dominiklohmann
created: 2023-04-18T12:48:45Z
pr: 3083
---

Automatic rebuilds now correctly consider only outdated or undersized
partitions.

The `--all` flag for the `rebuild` command now consistently causes all
partitions to be rebuilt, aligning its functionality with its documentation.
