---
title: "Introduce `--replace`, `--separate`, and `--yield` for contexts"
type: change
author: dominiklohmann
created: 2024-03-15T18:25:37Z
pr: 4040
---

The `enrich` and `lookup` operators now include the metadata in every context
object to accomodate the new `--replace` and `--separate` options. Previously,
the metadata was available once in the output field.

The `mode` field in the enrichments returned from the `lookup` operator is now
`lookup.retro`, `lookup.live`, or `lookup.snapshot` depending on the mode.

The `bloom-filter` context now always returns `true` or `null` for the context
instead of embedding the result in a record with a single `data` field.
