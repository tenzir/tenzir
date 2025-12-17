---
title: "Non-default databases in `to_clickhouse`"
type: bugfix
author: IyeOnline
created: 2025-07-21T09:55:43Z
pr: 5355
---

The `to_clickhouse` operator erroneously rejected `table` arguments of the form
`database_name.table_name`. This is now fixed, allowing you to write to
non-default databases.
