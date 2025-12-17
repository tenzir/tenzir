---
title: "Evict old caches when exceeding capacity limits"
type: change
author: dominiklohmann
created: 2025-02-10T13:14:42Z
pr: 4984
---

Unless specified explicitly, the `cache` has no more default capacity in terms
of number of events per cache. Instead, the node now tracks the global cache
capacity in number of bytes. This is limited to 1GiB by default, and can be
configured with the `tenzir.cache.capacity` option. For practical reasons, we
require at least 64MiB of caches.

The default `write_timeout` of caches increased from 1 minute to 10 minutes, and
can now be configured with the `tenzir.cache.lifetime` option.

The `/serve` endpoint now returns an additional field `state`, which can be one
of `running`, `completed`, or `failed`, indicating the status of the pipeline
with the corresponding `serve` operator at the time of the request.
