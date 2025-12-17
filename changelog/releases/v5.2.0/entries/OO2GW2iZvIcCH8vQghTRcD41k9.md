---
title: "Fix a crash in `to_clickhouse` and bump `clickhouse-cpp`"
type: bugfix
author: IyeOnline
created: 2025-05-23T12:03:49Z
pr: 5221
---

We fixed a bug in `to_clickhouse` that caused the operator to crash when
encountering lists.
