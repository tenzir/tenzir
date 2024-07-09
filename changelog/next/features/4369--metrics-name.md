The `metrics` operator now optionally takes a metric name as an argument. For
example, `metrics cpu` shows only CPU metrics. This is functionally equivalent
to `metrics | where #schema == "tenzir.metrics.cpu"`.
