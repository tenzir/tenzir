---
title: "Update query endpoint to use new pipeline executor"
type: change
author: jachris
created: 2023-03-31T10:02:16Z
pr: 3015
---

The `/query` REST endpoint no longer accepts an expression at the start of the
query. Instead, use `where <expr> | ...`.
