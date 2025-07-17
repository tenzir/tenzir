---
title: "Fix panic in `head 0 | write_json arrays_of_objects=true`"
type: feature
authors: dominiklohmann
pr: 5115
---

`write_json arrays_of_objects=true` now works correctly with an empty input,
returning an empty JSON array instead of running into a panic.
