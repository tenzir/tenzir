---
title: Add `auto_fill` option to `read_csv`, `read_tsv`, `read_ssv`, and `read_xsv`
type: feature
authors:
  - jachris
  - claude
created: 2026-05-18T00:00:00.000000Z
---

The `read_csv`, `read_tsv`, `read_ssv`, and `read_xsv` operators now accept an
`auto_fill=true` option. When set, the parser silently fills missing trailing
columns with `null` instead of emitting a warning, which is useful when working
with feeds that legitimately omit optional trailing fields.
