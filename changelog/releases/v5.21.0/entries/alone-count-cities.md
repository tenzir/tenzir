---
title: "Parse time to UNIX epoch"
type: change
author: raxyte
created: 2025-11-19T17:18:42Z
pr: 5571
---

The `parse_time()` function would earlier default to `1900` when the year was
unspecified. This has now been changed to `1970` to match the assumptions about
epoch in other parts of the language.
