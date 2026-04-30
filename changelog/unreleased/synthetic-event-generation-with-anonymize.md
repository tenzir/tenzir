---
title: Synthetic event generation with `anonymize`
type: feature
author: IyeOnline
created: 2026-04-30T12:56:56.187069Z
---

The new `anonymize` operator generates synthetic events that share the schemas
of its input. The operator first samples a configurable number of input events
to learn what schemas are present and to summarize their values, and then
replaces the input with generated events that match those schemas:

```tql
subscribe "events"
anonymize count=1000
```

By default, generated values follow the aggregate statistics of the sampled
input: null rates, list lengths, numeric ranges, time and duration ranges,
boolean and enum frequencies, string and blob lengths and byte frequencies, IP
address family frequencies, and subnet prefix length frequencies. Use
`fully_random=true` to ignore those statistics and instead pick values
uniformly from each type's full range. The optional `seed` argument makes
output reproducible.

Use `anonymize` to share representative event traces without leaking the
underlying values.
