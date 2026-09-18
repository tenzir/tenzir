---
title: Search function for recursive value matching
type: change
authors:
  - mavam
created: 2026-09-17T20:52:09.119723Z
---

The `search` function replaces `contains` for recursive value searches in records,
lists, and scalar values. The matching behavior and the `exact` and `ignore_case`
options are unchanged:

```tql
from {user: {name: "Alice"}}
found = search(user, "alice", ignore_case=true)
```

The old `contains` name still works but emits a deprecation warning. Replace
`contains(input, target)` with `search(input, target)`, or
`input.contains(target)` with `input.search(target)`.
