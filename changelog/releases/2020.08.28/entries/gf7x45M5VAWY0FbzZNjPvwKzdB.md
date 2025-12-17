---
title: "Remove set data type"
type: change
author: mavam
created: 2020-08-10T14:52:47Z
pr: 1010
---

The `set` type has been removed. Experience with the data model showed that
there is no strong use case to separate sets from vectors in the core. While
this may be useful in programming languages, VAST deals with immutable data
where set constraints have been enforced upstream. This change requires updating
existing schemas by changing `set<T>` to `vector<T>`. In the query language, the
new symbol for the empty `map` changed from `{-}` to `{}`, as it now
unambiguously identifies `map` instances.
