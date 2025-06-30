---
title: "TODO"
type: feature
authors: jachris
pr: 5312
---

The `ocsf::apply` operator now has an additional `TODO` option, which TODO. Most
notably, this applies to the `unmapped` field. For example, if `unmapped` is
`{x: 42}`, then `ocsf::apply` would normally JSON-encode it so that it ends up
with the value `"{\"x\": 42}"`. If `ocsf::apply TODO=TODO` is used instead, then
`unmapped` simply stays a record instead. Note that this means that the event
schema changes whenever the type of `unmapped` changes.
