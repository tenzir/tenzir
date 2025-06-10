---
title: "Correctly handle partition transforms without output"
type: change
authors: lava
pr: 2123
---

We fixed an issue where partition transforms that erase complete partitions
trigger an internal assertion failure.
