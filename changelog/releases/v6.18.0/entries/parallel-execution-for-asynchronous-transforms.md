---
title: Parallel execution for asynchronous transforms
type: change
authors:
  - aljazerzen
created: 2026-09-14T09:37:41.909578Z
---

The `dns_lookup`, `context_enrich`, `each`, and `python` operators now use the configured pipeline parallelism. Independent input batches run concurrently across operator instances, improving throughput for DNS requests, context lookups, child pipelines, and Python processing.

Python programs that retain state or perform side effects now run once per parallel operator instance. Use a parallelism degree of one when a program requires one continuous subprocess session.
