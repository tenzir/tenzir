---
title: Faster, lower-memory catalog loading at startup
type: feature
authors:
  - jachris
  - claude
prs:
  - 6368
created: 2026-06-18T10:35:07.122320Z
---

Nodes with a very large number of partitions now start up faster and with a
smaller memory footprint.

Partition synopses are now loaded from disk concurrently. The number of worker
threads defaults to the hardware concurrency and can be tuned with
`tenzir.index.load-concurrency`. This is especially effective on networked
storage (such as NFS), where loading is dominated by I/O latency that overlaps
across requests.

The new `tenzir.index.lazy-sketches` option defers loading Bloom-filter
sketches at startup and loads them on demand when a query needs them. Only
string and IP fields use Bloom filters; deferring them drastically lowers
resident memory and startup cost for nodes with very many partitions. When a
predicate would benefit from a deferred sketch, the catalog loads and checks
the surviving candidate partitions one at a time, so equality pruning on these
high-cardinality fields is preserved no matter how many partitions match —
only a single sketch is resident at a time. Loaded sketches are kept in a
memory-bounded LRU cache (`tenzir.index.sketch-cache-bytes`, default 1 GiB) to
speed up later queries; the budget caps that warm set, not how much can be
pruned. Loading reads the partition's local `.mdx`; on remote stores the
partition is conservatively treated as a candidate instead (never a false
negative). Numeric and duration min/max synopses and time synopses are never
deferred, so range pruning—for example on a timestamp field—is unaffected.

The new `tenzir.index.skip-synopsis-verification` option skips the recursive
FlatBuffers verification of partition synopses when reading them at startup,
verifying them once when they are written instead. Verification walks the
entire buffer and faults in all of its pages—including sketch payloads that are
never decoded—so skipping it is the dominant startup saving on networked
storage. It trades robustness against on-disk corruption for speed and should
only be enabled when the storage backend is trusted.

Loading also avoids a `realpath` and several `stat` calls per partition by
resolving the base directories once and reusing file sizes gathered during the
initial directory scan, and it now reports progress periodically for large
catalogs.
