---
title: Streaming windowed and periodic snapshot joins
type: feature
authors:
  - mavam
created: 2026-11-24T11:30:00.000000Z
---

The `join` operator now also joins two live streams and enriches a stream against a periodically reloaded snapshot, in addition to joining against a finite right-hand source.

Set `refresh=<duration>` to reload the right-hand subpipeline in the background on a schedule. The operator waits for the initial snapshot, then atomically swaps in each reload that completes cleanly. If a reload fails or exceeds `max_memory`, the operator emits a warning and keeps the previous snapshot:

```tql
subscribe "flows"
join src_ip == ip, kind="left", refresh=15m, into=asset {
  from_file "assets.json" { read_json }
}
```

Set `before`, `after`, or `within` to correlate events from two unbounded streams within a time window. By default, the window uses arrival time. With `on=<expr>` or `on=<left> == <right>`, it uses event time with per-side watermarks, and `tolerance` absorbs out-of-order events. Use `idle_timeout` to finalize rows when one stream goes quiet, and `pick="latest"`, `"earliest"`, or `"nearest"` to emit a single matching right event per left event, for example for backward as-of lookups:

```tql
subscribe "flows"
join ip, kind="left", before=1h, on=ts, pick="latest", into=lease {
  subscribe "dhcp"
}
```
