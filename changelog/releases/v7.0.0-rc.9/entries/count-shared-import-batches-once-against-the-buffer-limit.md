---
title: Count shared import batches once against the buffer limit
type: bugfix
authors:
  - tobim
created: 2026-11-12T14:00:24.667354Z
---

The `tenzir.max-buffered-bytes` limit now counts each batch of imported events once. Previously, a batch with events of several shapes counted once per shape, so the node flushed partitions long before the limit was actually reached and created many small partitions.

The node now creates fewer, larger partitions. Because the limit now reflects the memory that buffered events actually use, the node holds more events in memory before writing them to disk. To keep imports flowing, the node starts writing partitions at 80% of the limit and only slows down imports once the limit itself is reached.
