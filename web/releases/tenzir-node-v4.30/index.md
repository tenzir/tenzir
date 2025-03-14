---
title: "Tenzir Node v4.29: ClickHouse & Streamlined TLS"
slug: tenzir-node-v4.30
authors: [IyeOnline]
date: 2025-03-14
tags: [release, node]
comments: true
---

[Tenzir Node v4.30][github-release] introduces a `to_clickhouse` operator and
streamlines the TLS settings for all operators.

![Tenzir Node v4.29](tenzir-node-v4.20.svg)

[github-release]: https://github.com/tenzir/tenzir/releases/tag/v4.30.0

<!-- truncate -->

[operator-docs]: [../../docs/tql2/operators/to_clickhouse]

## ClickHouse Integration

Tenzir can now seamlessly write data to a [ClickHouse](https://clickhouse.com/)
table, using the new [`to_clickhouse`][operator-docs] operator. The operator
uses ClickHouse's C++ client library [clickhouse-cpp](https://github.com/ClickHouse/clickhouse-cpp)
to efficiently insert blocks in a columnar fashion. It supports all of Tenzir's
types and can both create tables as well as append to existing ones.

```tql
from { i: 42, d: 10.0, b: true, l: [42], r:{ s:"string" } }
to_clickhouse table="example", primary=i
```

## Streamlined TLS Settings

Many operators feature some TLS settings, that all worked very similar, but not
identical. With v4.30, the TLS settings are now are now identical between all
operators and all share the same semantics.

Additionally, there is a new configuration option `tenzir.cacert`, which allows
setting of a global default for the CA certificate file. The default value will
be chosen appropriately for the system the node runs on. Of course the per-operator
arguments still take precedence.

## Let's Connect!

Do you want to directly engage with Tenzir? Join our [Discord server][discord],
where we discuss projects and features and host our bi-weekly office hours
(every second Tuesday at 5 PM CET). Regardless of whether you just want to hang
out or have that one very specific question you just need answered, you are always
welcome!

[discord]: /discord
[changelog]: /changelog#v4230
