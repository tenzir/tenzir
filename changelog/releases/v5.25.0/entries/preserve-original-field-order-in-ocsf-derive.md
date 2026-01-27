---
title: Preserve original field order in ocsf::derive
type: change
authors:
  - mavam
  - claude
pr: 5673
created: 2026-01-20T15:26:52.087067Z
---

The `ocsf::derive` operator now preserves original field order instead of
reordering alphabetically. Derived enum/sibling pairs are inserted at the
position of the first field, ordered alphabetically within each pair (e.g.,
`activity_id` before `activity_name`). Non-OCSF fields remain at their original
positions.

For example, given the input:

```tql
{foo: 1, class_uid: 1001}
```

The output is now:

```tql
{foo: 1, class_name: "...", class_uid: 1001}
```

Previously, the output was alphabetically sorted:

```tql
{class_name: "...", class_uid: 1001, foo: 1}
```
