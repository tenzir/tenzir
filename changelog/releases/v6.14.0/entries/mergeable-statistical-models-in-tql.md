---
title: Mergeable statistical models and distribution comparisons
type: feature
authors:
  - mavam
created: 2026-08-24T16:25:24.749908Z
---

TQL now provides statistical models that you can inspect, store as ordinary
records, merge across pipelines, and compare for distribution drift. The new
aggregation functions create fixed-width numeric [histograms], exact
categorical [frequency tables], approximate [t-digests], and approximate
distinct counts with [HyperLogLog]:

```tql
summarize \
  size_distribution = histogram(size, bins=20, width=500.0),
  action_counts = frequency_table(action),
  latency_distribution = tdigest(latency_ms),
  distinct_users = hll(user_id)
```

Each model is a versioned TQL record, so you can send it through a pipeline or
store it in a lookup table like other data. Use `model_merge` to combine models
from separate windows, nodes, or stored records.

The models include dedicated query functions:

- `histogram_bucket` returns the bucket and count for a numeric value.
- `frequency_table_count` returns the exact count for a categorical value.
- `tdigest_quantile` and `tdigest_cdf` query an approximate distribution.
- `hll_cardinality` estimates the number of distinct values.

You can compare compatible histogram and frequency-table models with
[Jensen-Shannon divergence][jensen-shannon]. You can compare t-digests with
[Kolmogorov-Smirnov] or [Wasserstein] distance:

```tql
from {
  baseline_sizes: histogram([100.0, 200.0, 300.0], bins=4, width=100.0),
  current_sizes: histogram([100.0, 700.0, 800.0], bins=4, width=100.0),
  baseline_latency: tdigest([10.0, 12.0, 15.0]),
  current_latency: tdigest([10.0, 40.0, 50.0]),
}
select \
  size_drift = model_divergence(baseline_sizes, current_sizes, method="jensen_shannon"), \
  latency_shift = model_distance(baseline_latency, current_latency, method="wasserstein")
```

This produces:

```tql
{
  size_drift: 0.46209812037329684, // Investigate the newly observed large sizes
  latency_shift: 21.0, // Alert if this shift exceeds your latency tolerance
}
```

New generic distribution functions also operate directly on lists:

- `jensen_shannon` compares aligned weight vectors.
- `ecdf` evaluates an exact empirical CDF.
- `kolmogorov_smirnov` compares numeric or temporal samples.
- `wasserstein` compares numeric or temporal samples. For duration and
  timestamp samples, it returns a duration.

```tql
from {
  divergence: jensen_shannon([10, 20, 0], [5, 20, 5]),
  probability: ecdf([1, 2, 2, 4], 2),
  sample_distance: kolmogorov_smirnov([1, 2, 3], [2, 3, 4]),
  time_shift: wasserstein([1s, 2s, 3s], [2s, 3s, 4s]),
}
```

[distinct counts]: https://en.wikipedia.org/wiki/Count-distinct_problem
[frequency tables]: https://en.wikipedia.org/wiki/Frequency_distribution
[histograms]: https://en.wikipedia.org/wiki/Histogram
[HyperLogLog]: https://en.wikipedia.org/wiki/HyperLogLog
[jensen-shannon]: https://en.wikipedia.org/wiki/Jensen%E2%80%93Shannon_divergence
[Kolmogorov-Smirnov]: https://en.wikipedia.org/wiki/Kolmogorov%E2%80%93Smirnov_test
[t-digests]: https://arxiv.org/abs/1902.04023
[Wasserstein]: https://en.wikipedia.org/wiki/Wasserstein_metric
