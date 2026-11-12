---
title: Native MySQL semantics for operators after from_mysql
type: change
authors:
  - mavam
created: 2026-11-04T07:31:09.548594Z
---

`from_mysql` now hands MySQL the operators that follow it in a `table` read, as far as MySQL can spell them: `where`, `select`, `summarize`, `sort`, `head`, `top` or `rare` followed by `head`, and a `drop` after a `select`. Those operators leave the pipeline and run in MySQL with MySQL's semantics. Text compares, groups, and sorts under the collation of its column, which often ignores case and accents. `NULL` sorts first, and `name != "alpha"` no longer keeps rows where `name` is `NULL`:

```tql
from_mysql table="users", host="db.example.com", database="app"
where name == "alice" and created > 2025-01-01
select name, created
sort -created
head 10
```

MySQL can now use its indexes for these filters. Times compare in UTC: a `table` read sets the session's time zone to UTC, so `TIMESTAMP` columns also arrive in UTC. A `live` read hands MySQL its filters only. After a restore, a `table` read runs its query again and skips the events that it emitted before, which assumes that MySQL returns the rows in the same order, as a `sort` guarantees.

Use `pushdown=false` to keep every operator in the pipeline with TQL semantics:

```tql
from_mysql table="users", host="db.example.com", database="app", pushdown=false
where name == "alice"
```
