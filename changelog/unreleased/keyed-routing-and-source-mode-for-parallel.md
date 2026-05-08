---
title: Keyed routing and source mode for `parallel`
type: feature
author: jachris
pr: 5821
created: 2026-04-30T13:01:14.07634Z
---

The `parallel` operator gains two enhancements:

The `jobs` argument is now optional and defaults to the number of available
CPU cores:

```tql
subscribe "events"
parallel {
  parsed = data.parse_json()
}
```

The new `route_by` argument routes events to workers deterministically by
key. Events with the same `route_by` value always go to the same worker,
which is required for stateful subpipelines like `deduplicate` or
`summarize`:

```tql
subscribe "events"
parallel route_by=src_ip {
  deduplicate src_ip, dst_ip, dst_port
}
```

Additionally, `parallel` may now be used as a source operator (without
upstream input). This spawns multiple independent instances of the
subpipeline, which is useful for running the same source pipeline with
concurrent connections.
