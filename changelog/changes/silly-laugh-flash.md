---
title: "Formatting `ip` and `subnet` values in `to_amazon_security_lake`"
type: bugfix
authors: raxyte
pr: 5387
---

The `to_amazon_security_lake` operator now correctly formats `ip` and `subnet`
values as strings and formats timestamps using millisecond precision, similar
to the Security Lake built-in sources.
