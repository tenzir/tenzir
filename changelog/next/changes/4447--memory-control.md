We've made some changes that optimize Tenzir's memory usage. Pipeline operators
that emit very small batches of events or bytes at a high frequency now use less
memory. The `serve` operator's internal buffer is now soft-capped at 1Ki instead
of 64Ki events, aligning the buffer size with the default upper limit for the
number of events that can be fetched at once from `/serve`. The `export`,
`metrics`, and `diagnostics` operators now handle back pressure better and
utilize less memory in situations where the node has many small partitions. For
expert users, the new `tenzir.demand` configuration section allows for
controlling how eagerly operators demand input from their upstream operators.
Lowering the demand reduces the peak memory usage of pipelines at some
performance cost.
