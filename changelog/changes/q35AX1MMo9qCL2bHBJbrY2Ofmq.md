---
title: "Update query endpoint to use new pipeline executor"
type: change
authors: jachris
pr: 3015
---

The `/query` REST endpoint no longer accepts an expression at the start of the
query. Instead, use `where <expr> | ...`.
