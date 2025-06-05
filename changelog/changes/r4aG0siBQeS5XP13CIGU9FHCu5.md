---
title: "Introduce and make use of the timestamp type"
type: change
authors: tobim
pr: 1388
---

The special meaning of the `#timestamp` attribute has been removed from the
schema language. Timestamps can from now on be marked as such by using the
`timestamp` type instead. Queries of the form `#timestamp <op> value` remain
operational but are deprecated in favor of `:timestamp`. Note that this change
also affects `:time` queries, which aren't supersets of `#timestamp` queries any
longer.
