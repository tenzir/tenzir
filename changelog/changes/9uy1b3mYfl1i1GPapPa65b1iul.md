---
title: "Replace 'nil' with 'null'"
type: change
authors: Dakostu
pr: 2999
---

The non-value literal in expressions has a new syntax: `null` replaces its old
representation `nil`. For example, the query `x != nil` is no longer valid; use
`x != null` instead.
