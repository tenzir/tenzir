---
title: "Remove port type"
type: change
author: mavam
created: 2020-11-23T13:46:50Z
pr: 1187
---

The `port` type is no longer a first-class type. The new way to represent
transport-layer ports relies on `count` instead. In the schema, VAST ships with
a new alias `type port = count` to keep existing schema definitions in tact.
However, this is a breaking change because the on-disk format and Arrow data
representation changed. Queries with `:port` type extractors no longer work.
Similarly, the syntax `53/udp` no longer exists; use `count` syntax `53`
instead. Since most `port` occurrences do not carry a known transport-layer
type, and the type information exists typically in a separate field, removing
`port` as native type streamlines the data model.
