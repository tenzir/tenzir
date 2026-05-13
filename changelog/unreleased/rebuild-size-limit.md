---
title: Rebuild size limit
type: bugfix
authors:
  - tobim
  - codex
created: 2026-05-11T12:33:13.375284Z
pr: 6155
---

Rebuilds now avoid loading too much data into memory at once. Partitions store
their approximate in-memory size in metadata, and the new
`tenzir.rebuild.max-size` setting and `tenzir rebuild --max-size` option make
rebuild admit only partitions whose estimated input and output buffers still
fit into the current memory budget:

```sh
tenzir rebuild --max-size=512MiB
```

By default, Tenzir uses 50% of the currently available system memory. Set the
limit to `0` to restore the previous unlimited behavior. For existing
partitions without size metadata, rebuild estimates the size from measured
partitions with the same schema, falling back to an event-count heuristic only
when no schema-specific measurement exists.
