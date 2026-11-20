---
title: Non-ASCII varchar text from SQL Server
type: bugfix
authors:
  - mavam
created: 2026-11-20T14:05:38.191492Z
---

`from_microsoft_sql` now decodes `varchar`, `char`, and `text` values by the code page of their collation, so non-ASCII text such as `élan` arrives as valid UTF-8 instead of raw code page bytes. Previously, string functions in the pipeline returned `null` for such values.
