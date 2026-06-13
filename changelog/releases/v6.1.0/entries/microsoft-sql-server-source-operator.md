---
title: Microsoft SQL Server source operator
type: feature
authors:
  - tobim
  - codex
prs:
  - 6221
created: 2026-05-26T08:14:41.788782Z
---

The new `from_microsoft_sql` operator reads rows from Microsoft SQL Server:

```tql
from_microsoft_sql table="dbo.users",
                   host="sql.example.net",
                   user="tenzir",
                   password=secret("mssql-password"),
                   database="telemetry"
```

It supports table reads, custom `sql` or `query` statements, schema inspection with `show="tables"` and `show="columns"`, SQL authentication, secret-backed passwords, TLS, and live polling via integer tracking columns. Result decoding covers SQL Server scalar values including integers, booleans, floats, decimals, money values, strings, binary data, `uniqueidentifier`, date/time values, XML, and JSON stored as text.
