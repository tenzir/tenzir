---
title: "Don't abort startup if individual partitions fail to load"
type: bugfix
authors: tobim
pr: 2515
---

The `rebuild` command, automatic rebuilds, and compaction are now much faster,
and match the performance of the `import` command for building indexes.
