---
title: Filters no longer fail for events that earlier filters drop
type: bugfix
authors:
  - mavam
created: 2026-10-13T09:05:16.398932Z
---

A filter that reads a field which an earlier operator assigns stays behind that operator. The optimizer could still move a later filter in front of it, which then also evaluated the later filter for events that the earlier one drops. If the later filter could fail, such as `x.slice(stride=-1)` for a string, the pipeline failed for events that it would never have seen:

```tql
from {x: "abc"}
c = random() < 0
where c
where x.slice(stride=-1) == ""
```

A filter that could fail now keeps its place behind the filters that stay.
