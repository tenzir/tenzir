---
title: Native SQL Server semantics for operators after from_microsoft_sql
type: change
authors:
  - mavam
created: 2026-11-04T09:34:25.07961Z
---

`from_microsoft_sql` now hands SQL Server the operators that follow it in a `table` read, as far as T-SQL can spell them: `where`, `select`, `summarize`, `sort`, `head`, `top` or `rare` followed by `head`, and a `drop` after a `select`. Those operators leave the pipeline and run in SQL Server with its semantics. Text compares, groups, and sorts under the collation of its column, which often ignores case and trailing spaces. `NULL` sorts first, and `name != "alpha"` no longer keeps rows where `name` is `NULL`:

```tql
from_microsoft_sql table="dbo.logins", host="db.example.com", database="audit"
where user == "admin" and ts > 2025-01-01
select user, ts
sort -ts
head 10
```

SQL Server can now use its indexes for these filters. Times compare as instants in UTC with every temporal type. T-SQL has no boolean values, so a bare `where flag` on a `bit` column stays in the pipeline, while `where flag == true` goes into the query. A `live` read hands SQL Server its filters only. The operator takes over operators before it knows the types of the columns, so a query that SQL Server rejects fails the pipeline, for example `max` of a `bit` column or a comparison of a legacy `text` column; use `pushdown=false` for such pipelines.

Use `pushdown=false` to keep every operator in the pipeline with TQL semantics:

```tql
from_microsoft_sql table="dbo.logins", host="db.example.com", database="audit", pushdown=false
where user == "admin"
```
