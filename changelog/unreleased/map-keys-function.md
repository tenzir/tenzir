---
title: '`map_keys` function'
type: feature
authors:
  - raxyte
prs: []
created: 2026-06-29T10:04:53Z
---

The new `map_keys` function renames the top-level fields of a record by applying
a lambda to each field name. This makes workflows like lowercasing HTTP header
names possible with `request = request.map_keys(key => key.to_lower())`.
