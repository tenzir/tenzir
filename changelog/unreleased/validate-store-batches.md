---
title: Optionally validate batches read from persisted stores
type: feature
authors:
  - raxyte
created: 2026-07-28T00:00:00.000000Z
---

The new `tenzir.validate-store-batches` option fully validates every Arrow
record batch read from a persisted Feather store. This helps diagnose
on-disk corruption during exports and rebuilds, including invalid values such
as malformed UTF-8 that structural validation does not detect. The option is
disabled by default because full validation scans all values in every batch.
