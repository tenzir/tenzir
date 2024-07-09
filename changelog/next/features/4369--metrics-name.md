The `metrics` operator now optionally takes a metric name as an argument. For
example, `metrics cpu` only shows CPU metrics. This is functionally equivalent
to `metrics | where #schema == "tenzir.metrics.cpu"`.
