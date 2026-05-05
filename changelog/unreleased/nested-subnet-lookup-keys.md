---
title: Nested subnet lookup keys
type: feature
authors:
  - IyeOnline
  - codex
created: 2026-04-30T12:22:36.530493Z
---

Lookup table contexts can now match IP addresses inside record keys against subnet values in stored record keys.

For example, a lookup table entry keyed by a record such as `{src: 10.0.0.0/8, dst: 192.168.0.0/16}` can now enrich events looked up with `{src: 10.1.2.3, dst: 192.168.1.10}`. When multiple nested subnets match, Tenzir uses the most specific subnet match.

```tql title="Add subnets to the context"
from {
  network: {src: 10.0.0.0/8, dst: 192.168.0.0/16},
  label: "internal"
}
context::update "networks", key=network, value=label
```

```tql title="Lookup with IPs"
from {
  network: {src: 10.1.2.3, dst: 192.168.1.10}
}
context::enrich "networks", key=network, into=classification
```
```tql
{
  network: {
    src: 10.1.2.3,
    dst: 192.168.1.10,
  },
  classification: "internal",
}
```
