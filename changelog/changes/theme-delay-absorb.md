---
title: "Handling of type conflicts when reading data"
type: change
authors: jachris
pr: 5405
---

Tenzir requires all items of a list to have the same type. As a result, items in
lists that contain different types (such as `[1, "test"]`) are cast to the
common type `string`. Previously, all items were stored with their JSON
representation, leading to the result `["1", "\"test\""]`. Now, only lists and
record are stored as JSON, and strings are preserved without extra quotes. Thus,
the new output is `["1", "test"]`.
