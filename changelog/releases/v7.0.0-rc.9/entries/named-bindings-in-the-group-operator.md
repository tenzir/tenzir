---
title: Named bindings in the group operator
type: feature
authors:
  - mavam
created: 2026-11-25T20:07:25.07006Z
---

The `group` operator now accepts one or more `$name=<expr>` bindings, exposing each key as a scoped `$name` variable inside its subpipeline:

```tql
group $hour=ts.floor(1h), $ip=src_ip {
  summarize first=min(ts), last=max(ts), total=sum(bytes), n=count()
  ts = $hour
  ip = $ip
}
```

Named bindings avoid packing and unpacking a record through `$group`, keep an enclosing `$group` variable accessible in nested `group` blocks, and preserve the existing single-key `group <expr> { ... }` shorthand.
