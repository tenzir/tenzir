---
title: "Fix cache eviction always happening on maximally large caches"
type: bugfix
authors: Avaq
pr: 5039
---

We fixed a bug in the `cache` operator that caused caches that were capped just
short of the `tenzir.cache.capacity` option to still get evicted immediately.
