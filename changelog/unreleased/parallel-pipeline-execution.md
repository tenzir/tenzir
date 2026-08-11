---
title: Parallel pipeline execution
type: feature
authors:
  - aljazerzen
created: 2026-08-06T12:03:03.342771Z
---

Pipelines can now run CPU-bound operators on multiple cores.

Until now, the only way to use more than one core inside a pipeline was to wrap a
subpipeline in `parallel { … }`. This had problems with some operators like
`summarize` and `deduplicate`, which would produces partial results for each
parallel leg.

Now Tenzir parallelizes the individual operators of a pipeline, and it
preserves their semantics while doing so.

Parallelism is off by default. Turn it on for a pipeline with a
`// parallelism:` comment in its leading comment lines:

```tql
// parallelism: 4
from_file ...
where dest_port == 443
bytes = orig_bytes + resp_bytes
summarize dest_ip, total=sum(bytes)
to_clickhouse ...
```

Here, `where`, the assigment and the `summarize` would run with 4 parallel
instances. Each `summarize` instance is reponsible for aggregation of a fourth
of all possible `dest_ip`.

Parallelism gives up ordering: events overtake each other as they flow through
concurrent instances, so a parallel pipeline may emit them in a different order
than it received them. Use `sort` if you need a specific order downstream, and
leave parallelism disabled when the incoming order carries meaning.

The `tenzir` binary also accepts `--parallelism` with the same values, which
applies to pipelines that carry no comment.
