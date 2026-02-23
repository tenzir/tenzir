---
title: Slice function extended to support lists
type: feature
authors:
  - mavam
  - codex
pr: 5819
created: 2026-02-22T18:04:20.448605Z
---

The `slice` function now supports `list` types in addition to `string`. You can slice lists using the same `begin`, `end`, and `stride` parameters. Negative stride values are now supported for lists, letting you reverse or step backward through list data. String slicing continues to require a positive `stride`.

Example usage with lists:

- `[1, 2, 3, 4, 5].slice(begin=1, end=4)` returns `[2, 3, 4]`
- `[1, 2, 3, 4, 5].slice(stride=-1)` returns the list in reverse order
- `[1, 2, 3, 4, 5].slice(begin=1, end=5, stride=-2)` returns `[5, 3]`
