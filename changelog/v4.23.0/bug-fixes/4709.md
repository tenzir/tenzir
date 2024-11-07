TQL2's `summarize` now returns a single event when used with no groups and no
input events just like in TQL1, making `from [] | summarize count=count()`
return `{count: 0}` instead of nothing.
