---
title: Unresponsive nodes during catalog lookups
type: bugfix
authors:
  - tobim
created: 2026-10-14T10:00:00Z
---

Nodes with large catalogs stay responsive while catalog lookups run. Since
Tenzir v6.19.0, a lookup over many partitions could occupy the node's shared
worker threads for its whole duration. On nodes with few CPU cores this delayed
ingest, delivered metrics minutes apart, and left retro exports such as
`diagnostics` or `export` waiting for data indefinitely. Catalog lookups and the
importer now run on dedicated threads, named `tnz.lookup-<n>` and
`tnz.importer`.

The log message that reports how long a catalog lookup took is emitted at the
verbose level again.
