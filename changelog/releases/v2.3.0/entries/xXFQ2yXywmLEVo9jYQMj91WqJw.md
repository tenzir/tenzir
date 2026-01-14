---
title: "Don't abort startup if individual partitions fail to load"
type: bugfix
author: tobim
created: 2022-09-23T12:15:50Z
pr: 2515
---

The `rebuild` command, automatic rebuilds, and compaction are now much faster,
and match the performance of the `import` command for building indexes.
