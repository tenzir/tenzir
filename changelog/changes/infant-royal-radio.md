---
title: "Lambdas in `map` and `where` can capture surrounding fields"
type: feature
authors: raxyte
pr: 5457
---

Lambda expressions in the `map` and `where` functions can now capture and access fields from
the surrounding context, enabling more powerful data transformations.

For example:

```tql
from {
  host: "server1",
  ports: [80, 443, 8080]
}
ports = ports.map(p => {host: host, port: p})
```

```tql
{
  host: "server1",
  ports: [
    {
      host: "server1",
      port: 80,
    },
    {
      host: "server1",
      port: 443,
    },
    {
      host: "server1",
      port: 8080,
    },
  ],
}
```
