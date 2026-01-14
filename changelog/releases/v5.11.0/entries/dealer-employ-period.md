---
title: "Fixed issue with table creation in `to_clickhouse`"
type: bugfix
author: IyeOnline
created: 2025-07-24T09:23:02Z
pr: 5360
---

Multiple `to_clickhouse` operators can now attempt to create the same ClickHouse
table at the same time without an error.
