---
title: "Establish subtyping relationships for type extractors"
type: change
authors: tobim
pr: 1446
---

The type extractor in the expression language now works with type aliases. For
example, given the type definition for port from the base schema `type port =
count`, a search for `:count` will also consider fields of type `port`.
