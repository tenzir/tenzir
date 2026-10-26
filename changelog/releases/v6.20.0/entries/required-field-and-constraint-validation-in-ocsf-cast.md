---
title: Required field and constraint validation in the ocsf_cast function
type: bugfix
authors:
  - zedoraps
created: 2026-10-21T08:36:10.589395Z
---

The `ocsf_cast` function now warns when an event lacks a field that its OCSF class or object marks as required, or when it violates one of the schema's `at_least_one` or `just_one` constraints. Previously, such events passed through silently even though they are not valid OCSF. The `ocsf_cast` operator does not perform these checks.

For example, `this = ocsf_cast(this)` on an Authentication event without a `user` or with a `user` that has none of its identifying fields now reports:

```text
warning: required field `user` is missing
warning: `user` needs at least one of `account`, `name`, `uid`, but has none
```

Required fields inside an optional object only count when that object is present. The new checks only add warnings and never drop events, so existing pipelines keep running. Fix the mapping to populate the reported fields.
