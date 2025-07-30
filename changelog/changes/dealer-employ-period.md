---
title: "Fixed issue with table creation in `to_clickhouse`"
type: bugfix
authors: IyeOnline
pr: 5360
---

Multiple `to_clickhouse` operators can now attempt to create the same ClickHouse
table at the same time without an error.
