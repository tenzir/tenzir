---
title: Track partitions out-of-core with `--external-catalog`
type: feature
authors:
  - lava
created: 2026-06-18T00:00:00.000000Z
---

The new `--external-catalog` option points the node at a JSON manifest that
lists partitions together with their schemas, import time ranges, and per-type
min/max synopses. The node keeps only this lightweight metadata in memory and
prunes queries directly from it, without reading the partitions' `.mdx` synopsis
files. This keeps the catalog's memory footprint small for nodes that track a
large number of partitions.
