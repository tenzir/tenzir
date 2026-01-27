---
title: "Fix a crash in `to_clickhouse`"
type: bugfix
author: IyeOnline
created: 2025-06-03T06:40:58Z
pr: 5231
---

We fixed an issue when trying to send lists in `to_clickhouse` that would cause
the ClickHouse server to drop the connection.
