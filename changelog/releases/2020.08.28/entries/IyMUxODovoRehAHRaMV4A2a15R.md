---
title: "Rename vector to list"
type: change
author: mavam
created: 2020-08-10T16:04:02Z
pr: 1016
---

The `vector` type has been renamed to `list`. In an effort to streamline the
type system vocabulary, we favor `list` over `vector` because it's closer to
existing terminology (e.g., Apache Arrow). This change requires updating
existing schemas by changing `vector<T>` to `list<T>`.
