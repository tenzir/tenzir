---
title: "Correctly handle partition transforms without output"
type: change
author: lava
created: 2022-03-01T11:33:36Z
pr: 2123
---

We fixed an issue where partition transforms that erase complete partitions
trigger an internal assertion failure.
