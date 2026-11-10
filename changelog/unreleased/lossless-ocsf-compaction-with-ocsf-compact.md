---
title: Lossless OCSF compaction with ocsf_compact
type: feature
authors:
  - zedoraps
created: 2026-10-28T09:35:08.301077Z
---

The new `ocsf_compact` function removes OCSF enum strings that `ocsf_derive` can rebuild from their integer counterparts. Use it to store only the ids and restore the names when reading the data:

```tql
this = ocsf_compact(this)
```

For example, `{activity_id: 1, activity_name: "Logon", severity_id: 1, severity: "Informational"}` becomes `{activity_id: 1, severity_id: 1}`. The function works on nested records and enum lists, such as `answers[].flags`.

The compaction is lossless: running `ocsf_derive` afterward restores the original event. To make this possible, the function keeps every string that `ocsf_derive` cannot rebuild exactly. This includes the data source specific value of an `Other` (`99`) enum, values that do not match their integer, unknown integers, and strings without an integer.

With `uids=true`, the function also removes `category_uid` and `type_uid` where `ocsf_derive` computes them from `class_uid` and `activity_id`:

```tql
this = ocsf_compact(this, uids=true)
```

Both fields are required in OCSF, so the events are only valid OCSF again after `ocsf_derive` restored them.
