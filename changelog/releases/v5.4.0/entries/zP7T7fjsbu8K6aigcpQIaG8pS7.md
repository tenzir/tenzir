---
title: "Format strings"
type: feature
author: [jachris, IyeOnline]
created: 2025-06-11T11:18:20Z
pr: 5254
---

TQL now supports format strings as you might know them from other languages like
Python. Format strings allow you to flexibly construct strings in a very
succinct way by using a pair of braces within an `f"…"` string.

For example, assume that you have events with two integer fields, `found` and
`total`. We can construct a message from this as follows:

```tql
percent = round(found / total * 100).string()
message = "Found " + found.string() + "/" + total.string() + " => " + percent + "%"
```

Using the new format strings, this simply becomes

```tql
percent = round(found / total * 100)
message = f"Found {found}/{total} => {percent}%"
```

You can also use arbitrary expressions inside `{` to simplify this even further:

```tql
message = f"Found {found}/{total} => {round(found / total * 100)}%"
```

If you ever need an actual `{` in your format string, you can use `{{`. The same
goes for the closing brace `}`, which needs to be written as `}}` within format
strings.
