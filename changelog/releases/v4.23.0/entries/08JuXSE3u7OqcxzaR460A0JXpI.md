---
title: "Implement `in` for `list_type`"
type: feature
author: raxyte
created: 2024-10-31T08:59:24Z
pr: 4691
---

The relational operator `in` now supports checking for existence of an element
in a list. For example, `where x in ["important", "values"]` is functionally
equivalent to `where x == "important" or x == "values"`.
