---
title: "Remove the `#field` meta extractor"
type: change
author: dominiklohmann
created: 2022-12-09T23:36:38Z
pr: 2776
---

The `#field` meta extractor no longer exists. Use `X != null` over `#field ==
"X"` to check for existence for the field `X`.
