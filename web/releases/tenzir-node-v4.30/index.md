---
title: "Tenzir Node v4.30: ClickHouse Integration"
slug: tenzir-node-v4.30
authors: [IyeOnline]
date: 2025-03-18
tags: [release, node]
comments: true
---

[Tenzir Node v4.30][github-release] introduces a [`to_clickhouse`][operator-docs]
operator and streamlines the TLS settings for all operators.

![Tenzir Node v4.30](tenzir-node-v.4.30.svg)

[github-release]: https://github.com/tenzir/tenzir/releases/tag/v4.30.0

[operator-docs]: /next/tql2/operators/to_clickhouse
[clickhouse-website]: https://clickhouse.com/
[clickhouse-cpp-lib]: https://github.com/ClickHouse/clickhouse-cpp

<!-- truncate -->

## ClickHouse Integration

Tenzir can now seamlessly write data to a [ClickHouse][clickhouse-website]
table, using the new [`to_clickhouse`][operator-docs] operator. The operator
uses ClickHouse's C++ client library [clickhouse-cpp][clickhouse-cpp-lib]
to efficiently insert blocks in a columnar fashion. It supports all of Tenzir's
types and can both create tables as well as append to existing ones:

```tql
from { i: 42, d: 10.0, b: true, l: [42], r:{ s:"string" } }
to_clickhouse table="example", primary=i
```

## Streamlined TLS Settings

Many operators feature some TLS settings, that all worked very similar, but not
identical. With v4.30, the TLS settings are now are now identical between all
operators and all share the same semantics.

Additionally, there is a new configuration option `tenzir.cacert`, which allows
setting of a global default for the CA certificates file. The default value will
be chosen appropriately for the system the node runs on. Of course the per-operator
arguments still take precedence.

```yaml title="Set a cacert default in the config"
# /opt/tenzir/etc/tenzir/tenzir.yaml
tenzir:
  cacert: path/to/custom.bundle
```

:::warning TLS by default
With these changes we also enabled TLS by default on most operators. This may
cause some pipelines that rely on TLS not being enabled to stop working. If you
want to explicitly disable TLS, you can still do so by providing `tls=false` to
these operators.
:::

## Pipeline Metrics

Tenzir previously only featured very detailed operator metrics, which provided
a whole lot of detail, but were both cumbersome to work with and fairly expensive
to collect.

This release introduces [pipeline metrics](/next/tql2/operators/metrics#tenzirmetricspipeline),
which give you information about the throughput of entire pipelines. These metrics
are much easier to work with, for example to create Dashboards:

```tql title="Ingress of all Pipelines"
metrics "pipeline"
chart_line x=timestamp, y={
  "Ingress Gb/s": mean(ingress.bytes / 1G / ingress.duration.count_seconds()),
}, resolution=10s, group=pipeline_id, x_min = now()-1d
```

:::info Deprecation Notice
The old Operator Metrics are now deprecated and will be removed in some future
release.
:::

## Fixes & Updates

This release also contains a few small fixes. Most notably we enabled native
Google Cloud Storage support to TQL2 in the form of the [`load_gcs`][load_gcs]
and [`save_gcs`][save_gcs] operators. These are also supported using the `gs://`
URI scheme in the generic `from` and `to` operators.

[load_gcs]: /next/tql2/operators/load_gcs
[save_gcs]: /next/tql2/operators/save_gcs

## Let's Connect!

Join our [Discord server][discord], to ask questions, discuss features or just
hang out! Here we also host our bi-weekly office hours (every second Tuesday at 5 PM CET),
where we showcase the latest features, give sneak peaks into upcoming ones,
answer your questions or just discuss general tech topics!
We are looking forward to getting in touch with you!

[discord]: /discord
[changelog]: /changelog#v4230
