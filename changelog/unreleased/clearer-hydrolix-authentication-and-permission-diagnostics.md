---
title: Clearer Hydrolix authentication and permission diagnostics
type: bugfix
authors:
  - mavam
created: 2026-10-15T17:19:53.622607Z
---

The `from_hydrolix` operator now gives more relevant authentication and permission diagnostics. Authentication failures omit ClickHouse password-reset instructions while retaining the token hint. Missing `SHOW COLUMNS` grants now point to Hydrolix’s `show_columns_sql` permission for schema access, which `select_sql` also grants; missing `SELECT` grants still point to `select_sql`.
