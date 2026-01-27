---
title: "Replace 'nil' with 'null"
type: change
author: Dakostu
created: 2023-03-10T08:43:47Z
pr: 2999
---

The non-value literal in expressions has a new syntax: `null` replaces its old
representation `nil`. For example, the query `x != nil` is no longer valid; use
`x != null` instead.
