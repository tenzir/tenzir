---
title: "Abort the response promise if running into oversized partitions"
type: bugfix
authors: lava
pr: 2624
---

We fixed an indefinite hang that occurred when attempting to apply a pipeline to
a partition that is not a valid flatbuffer.
