---
title: "List manipulation with add and remove functions"
type: feature
authors: mavam
pr: 5471
---

Managing lists just got easier with two new functions that bring set-like
operations to your pipelines. Say goodbye to duplicate entries and unwanted
elements!

The `add` function ensures uniqueness when building lists. Perfect for
maintaining deduplicated threat intel feeds or collecting unique user sessions:

```tql
from {xs: [1, 2], y: 3},
     {xs: [1, 2], y: 1},
     {xs: [], y: 4}
select result = xs.add(y)
```

```tql
{result: [1, 2, 3]}
{result: [1, 2]}
{result: [4]}
```

The `remove` function cleans up your lists by eliminating all occurrences of
unwanted elements. Ideal for filtering out known-good domains from suspicious
activity logs or removing false positives from alert lists:

```tql
from {xs: [1, 2, 1, 3], y: 1},
     {xs: [4, 5], y: 6},
     {xs: [7, 7, 7], y: 7}
select result = xs.remove(y)
```

```tql
{result: [2, 3]}
{result: [4, 5]}
{result: []}
```

Both functions work seamlessly with any data type, from strings and numbers to
complex records. The `add` function performs set-insertion (no duplicates),
while `remove` eliminates every occurrence of the specified element. Together,
they make list management in your security pipelines cleaner and more
efficient!
