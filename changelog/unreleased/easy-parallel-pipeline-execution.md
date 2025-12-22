---
title: Easy parallel pipeline execution
type: feature
authors:
  - raxyte
created: 2025-12-23T09:37:18.145887Z
---

The `parallel` operator executes a pipeline across multiple parallel pipeline instances to improve throughput for computationally expensive operations. It automatically distributes input events across the pipeline instances and merges their outputs back into a single stream.

Use the `parallelism` parameter to specify how many pipeline instances to spawn. For example, to parse JSON in parallel across 4 pipeline instances:

```tql
from_file "input.ndjson"
read_lines
parallel jobs=4 {
  this = line.parse_json()
}
```

The operator splits large batches into smaller chunks based on the `split_threshold` parameter (default: 5000 events) for better distribution. This is particularly useful for CPU-intensive transformations where a single pipeline instance becomes a bottleneck.
