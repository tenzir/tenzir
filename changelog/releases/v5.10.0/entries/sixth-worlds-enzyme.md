---
title: "`kv` parser no longer produces empty fields"
type: change
author: IyeOnline
created: 2025-07-07T08:39:32Z
pr: 5313
---

Our Key-Value parsers (the `read_kv` operator and `parse_kv` function) previously
produced empty values if the `value_split` was not found.

With this change, a "field" missing a `value_split` is considered an extension
of the previous fields value instead:

```tql
from \
  {input: "x=1 y=2 z=3 4 5 a=6"},
this = { ...input.parse_kv() }
```
Previous result:
```tql
{x:1, y:2, z:"3", "4":"", "5":"", a:6}
```
New result:
```tql
{x:1, y:2, z:"3 4 5", a:6}
```
