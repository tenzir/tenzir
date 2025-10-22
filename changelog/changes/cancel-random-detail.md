---
title: "Reduced memory consumption during import"
type: change
authors: jachris
pr: [5532, 5533, 5535]
---

The memory usage while importing events has been significantly optimized.
Previously, importing would leave a trail of memory usage that only decreased
slowly over a period corresponding to `tenzir.active-partition-timeout`. Now,
events are properly released immediately after being written to disk, preventing
unnecessary memory accumulation.

We also eliminated redundant copies throughout the import path, reducing memory
usage by 2-4x depending on the dataset. Additionally, we optimized the memory
usage of buffered synopses, which are used internally when building indexes
during import. This optimization avoids unnecessary copies of strings and IP
addresses, roughly halving the memory consumption of the underlying component.
