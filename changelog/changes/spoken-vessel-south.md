---
title: "Improved list manipulation"
type: feature
authors: [mavam,IyeOnline]
pr: 5471
---

We have added two new functions that make managing set-like lists easier.

The `add` function ensures uniqueness when building lists. Perfect for
maintaining deduplicated threat intel feeds or collecting unique user sessions:

```tql
from {xs: [1]},
     {xs: [2]},
     {xs: []}
select result = xs.add(2)
```

```tql
{result: [1,2]}
{result: [2]}
{result: [2]}
```

The `remove` function cleans up your lists by eliminating all occurrences of
unwanted elements. Ideal for filtering out known-good domains from suspicious
activity logs or removing false positives from alert lists:

```tql
from {xs: [1, 2, 1, 3], y: 1},
     {xs: [4, 5], y: 1},
select result = xs.remove(y)
```

```tql
{result: [2, 3]}
{result: [4, 5]}
```
