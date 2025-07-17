---
title: "Use heterogenous lookup for hash index"
type: feature
authors: lava
pr: 796
---

The hash index has been re-enabled after it was outfitted with a new
[high-performance hash map](https://github.com/Tessil/robin-map/) implementation
that increased performance to the point where it is on par with the regular
index.
