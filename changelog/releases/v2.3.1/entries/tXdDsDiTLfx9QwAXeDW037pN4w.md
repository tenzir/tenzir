---
title: "Abort the response promise if running into oversized partitions"
type: bugfix
author: lava
created: 2022-10-13T17:17:33Z
pr: 2624
---

We fixed an indefinite hang that occurred when attempting to apply a pipeline to
a partition that is not a valid flatbuffer.
