---
title: "Preserving variants when using `ocsf::apply`"
type: feature
author: jachris
created: 2025-07-01T15:43:21Z
pr: 5312
---

The `ocsf::apply` operator now has an additional `preserve_variants` option,
which makes it so that free-form objects are preserved as-is, instead of being
JSON-encoded. Most notably, this applies to the `unmapped` field. For example,
if `unmapped` is `{x: 42}`, then `ocsf::apply` would normally JSON-encode it so
that it ends up with the value `"{\"x\": 42}"`. If `ocsf::apply
preserve_variants=true` is used instead, then `unmapped` simply stays a record.
Note that this means that the event schema changes whenever the type of
`unmapped` changes.
