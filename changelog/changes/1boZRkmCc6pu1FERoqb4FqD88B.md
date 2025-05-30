---
title: "Do not optimize `deduplicate`"
type: bugfix
authors: dominiklohmann
pr: 4379
---

We fixed a bug that caused `deduplicate <fields...> --distance <distance>` to
sometimes produce incorrect results when followed by `where <expr>` with an
expression that filters on the deduplicated fields.
