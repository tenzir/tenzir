---
title: "Introduce `--replace`, `--separate`, and `--yield` for contexts"
type: bugfix
author: dominiklohmann
created: 2024-03-15T18:25:37Z
pr: 4040
---

`drop` and `select` silently ignored all but the first match of the specified
type extractors and concepts. This no longer happens. For example, `drop :time`
drops all fields with type `time` from events.

Enriching a field in adjacent events in `lookup` and `enrich` with a
`lookup-table` context sometimes crashed when the lookup-table referred to
values of different types.

The `geoip` context sometimes returned incorrect values. This no longer happens.
