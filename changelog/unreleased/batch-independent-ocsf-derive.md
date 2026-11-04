---
title: Batch-independent list handling in ocsf_derive
type: bugfix
authors:
  - mavam
created: 2026-11-03T15:53:52.000000Z
---

`ocsf_derive` treats an enum pair as inconsistent when one of its fields is a list and the other is not, and keeps both fields unchanged. It used to apply this rule only when other events in the same batch had lists in both fields, so the result for an event could depend on the events around it. It now applies the rule to every event, so that `{activity_id: [1], activity_name: null}` keeps its `null` name regardless of the other events.
