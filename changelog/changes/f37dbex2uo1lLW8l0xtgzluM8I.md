---
title: "Fix bugs in `to_clickhouse` and improve diagnostics"
type: feature
authors: IyeOnline
pr: 5122
---

The `to_clickhouse` operator now supports `blob`s, which will be sent as an
`Array(UInt8)`.
