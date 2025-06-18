---
title: "Make the map type inaccessible to users"
type: change
authors: dominiklohmann
pr: 2976
---

The `map` type no longer exists: instead of `map<T, U>`, use the equivalent
`list<record{ key: T, value: U }>`.
