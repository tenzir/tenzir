---
title: Correct enrichments for complex key expressions
type: bugfix
authors:
  - IyeOnline
created: 2026-10-19T15:15:51.752188Z
---

With the old data model (`tenzir.nova: false`), `context_enrich` assigned enrichments to the wrong events whenever evaluating the `key` expression cut the batch into parts, which happens when the expression produces values of different types, for example through `if`/`else` or `else` with differently typed operands. The operator looks up the keys of each part separately but collected the results in reverse order.

For example, the following pipeline enriched the event looked up by IP with the entry for the hostname, and vice versa:

```tql
from {by_name: false, name: "web-1", ip: 10.0.0.1},
     {by_name: true, name: "web-1", ip: 10.0.0.1}
context_enrich "hosts", key=name if by_name else ip
```

A plain field as the key, such as `key=x`, was not affected: in the old data model, a column has exactly one type, so events in which `x` has different types already arrive in separate batches.
