---
title: Sequential pipelines without fused channels
type: change
authors:
  - aljazerzen
  - claude
created: 2026-08-19T09:56:56.49943Z
---

Pipelines that request no parallelism now behave exactly as if they carried a
`// parallelism: disabled` directive. This is what `--parallelism` documented
all along, but the effective default also fused the channels between
parallelizable operators, which made them process one item at a time
end-to-end. Such pipelines now use plain buffered channels instead.

Setting `fused=none` explicitly no longer restricts channels to a small buffer
either. It now consistently means "plain buffered channels throughout", at the
cost of more memory at a high degree of parallelism:

```tql
// parallelism: 6,fused=none
```

To get the previous default behavior for a single pipeline, request degree one
explicitly with `// parallelism: 1`, or set `tenzir.parallelism: 1` to get it
for a whole node.
