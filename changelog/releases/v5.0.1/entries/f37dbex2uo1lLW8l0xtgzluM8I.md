---
title: "Fix bugs in `to_clickhouse` and improve diagnostics"
type: feature
author: IyeOnline
created: 2025-04-22T12:06:16Z
pr: 5122
---

The `to_clickhouse` operator now supports `blob`s, which will be sent as an
`Array(UInt8)`.
