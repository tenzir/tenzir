---
title: "Make the `rebuild` command more reliable"
type: feature
author: dominiklohmann
created: 2022-08-16T08:59:12Z
pr: 2493
---

VAST now continuously rebuilds outdated and merges undersized partitions in the
background. The new option `vast.automatic-rebuild` controls how many resources
to spend on this. To disable this behavior, set the option to 0; the default is
1.

Rebuilding now emits metrics under the keys
`rebuilder.partitions.{remaining,rebuilding,completed}`. The `vast status
rebuild` command additionally shows information about the ongoing rebuild.
