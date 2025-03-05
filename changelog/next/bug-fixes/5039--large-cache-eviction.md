We fixed a bug in the `cache` operator that caused caches that were capped just
short of the `tenzir.cache.capacity` option to still get evicted immediately.
