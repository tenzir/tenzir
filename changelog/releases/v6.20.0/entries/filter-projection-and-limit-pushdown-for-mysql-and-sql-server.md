---
title: Filter, projection, and limit pushdown for MySQL and SQL Server
type: feature
authors:
  - mavam
created: 2026-10-02T20:28:15.659445Z
---

The `from_mysql` and `from_microsoft_sql` operators now push filters, projections, and limits into the query they send in `table` mode, so that the database returns only the rows and columns the pipeline needs:

```tql
from_microsoft_sql table="logins", host="db.example.com", database="audit"
where user == "admin" and result != "success"
select user, source
head 10
```

This sends a single `SELECT` with a `WHERE` clause, the two columns, and `TOP (10)`, instead of reading the whole table. Live mode pushes the filter and the columns into every poll.

Results stay the same as when the pipeline evaluates the operators itself. Text compares by bytes, as in TQL, so `user == "admin"` does not match `Admin` even though the default collations of both databases ignore case. Predicates that a database cannot evaluate exactly keep running in the pipeline, such as comparisons on `DECIMAL` columns in both databases, on `FLOAT` and temporal columns in MySQL, and on `datetime` columns in SQL Server. In MySQL, case-insensitive `starts_with` and `ends_with` use the database's lowercasing, which differs from TQL's case folding for a few characters, such as `ß`. A user-provided `sql` query stays as it is.
