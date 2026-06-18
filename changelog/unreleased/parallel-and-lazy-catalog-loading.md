---
title: Faster, lower-memory catalog loading at startup
type: feature
authors:
  - jachris
  - claude
created: 2026-06-18T10:35:07.122320Z
---

Nodes with a very large number of partitions now start up faster and with a
smaller memory footprint.

Partition synopses are now loaded from disk concurrently. The number of worker
threads defaults to the hardware concurrency and can be tuned with
`tenzir.index.load-concurrency`. This is especially effective on networked
storage (such as NFS), where loading is dominated by I/O latency that overlaps
across requests.

The new `tenzir.index.lazy-sketch-threshold` option (in bytes, `0` disables it)
makes the index skip deserializing large opaque synopses—most notably Bloom
filters—when loading the catalog. The corresponding fields remain registered,
so the catalog still considers them when answering queries (it may return more
candidate partitions, but never fewer), trading sketch-based pruning of
equality predicates on high-cardinality fields for drastically lower resident
memory and faster startup. Min/max and time synopses are always loaded, so
range pruning—for example on a timestamp field—is unaffected.
