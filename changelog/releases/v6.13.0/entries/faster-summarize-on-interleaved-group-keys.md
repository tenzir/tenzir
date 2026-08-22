---
title: Faster summarize on interleaved group keys
type: change
authors:
  - aljazerzen
  - claude
created: 2026-08-18T06:29:45.146116Z
---

The `summarize` operator is significantly faster on inputs whose group keys
arrive interleaved, such as a stream of connection logs grouped by
`src_ip, dest_ip` where consecutive events rarely belong to the same group:

```tql
summarize src_ip, dest_ip, bytes=sum(bytes), events=count()
```

Grouping wide events, or grouping by several fields at once, benefits most.
Inputs that already arrive sorted by their group key can be slightly slower,
most noticeably when nearly every event forms its own group.
