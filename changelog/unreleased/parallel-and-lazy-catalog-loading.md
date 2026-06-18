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
makes the index skip deserializing Bloom-filter sketches larger than the
threshold when loading the catalog. Only string and IP fields use Bloom
filters; the corresponding fields remain registered, so the catalog still
considers them when answering queries (it may return more candidate partitions,
but never fewer), trading sketch-based pruning of equality predicates on these
high-cardinality fields for drastically lower resident memory and faster
startup. Numeric and duration min/max synopses and time synopses are never
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
