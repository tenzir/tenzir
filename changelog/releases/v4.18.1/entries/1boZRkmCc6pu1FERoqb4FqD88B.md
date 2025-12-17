---
title: "Do not optimize `deduplicate`"
type: bugfix
author: dominiklohmann
created: 2024-07-11T15:13:30Z
pr: 4379
---

We fixed a bug that caused `deduplicate <fields...> --distance <distance>` to
sometimes produce incorrect results when followed by `where <expr>` with an
expression that filters on the deduplicated fields.
