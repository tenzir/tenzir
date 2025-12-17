---
title: "Make the map type inaccessible to users"
type: change
author: dominiklohmann
created: 2023-03-01T15:20:57Z
pr: 2976
---

The `map` type no longer exists: instead of `map<T, U>`, use the equivalent
`list<record{ key: T, value: U }>`.
