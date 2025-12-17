---
title: "Use heterogenous lookup for hash index"
type: feature
author: lava
created: 2020-03-16T10:52:49Z
pr: 796
---

The hash index has been re-enabled after it was outfitted with a new
[high-performance hash map](https://github.com/Tessil/robin-map/) implementation
that increased performance to the point where it is on par with the regular
index.
