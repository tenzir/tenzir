---
title: DNS result caching in `dns_lookup`
type: feature
author: mavam
pr: 6034
created: 2026-04-30T13:02:07.277857Z
---

The `dns_lookup` operator now caches DNS results and reuses them across
lookups. Forward-lookup results gain a `ttl` field that shows the remaining
lifetime of the cached answer:

```tql
from {host: "example.com"}
dns_lookup host
```

If Tenzir cannot initialize DNS resolution at all, the operator now emits an
error and stops instead of writing `null` results for every event.
Individual failed or timed-out lookups still produce `null`, as before.
