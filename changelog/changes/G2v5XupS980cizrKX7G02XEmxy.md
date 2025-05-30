---
title: "Remove the `#field` meta extractor"
type: change
authors: dominiklohmann
pr: 2776
---

The `#field` meta extractor no longer exists. Use `X != null` over `#field ==
"X"` to check for existence for the field `X`.
