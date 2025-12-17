---
title: "Establish subtyping relationships for type extractors"
type: change
author: tobim
created: 2021-03-15T13:55:55Z
pr: 1446
---

The type extractor in the expression language now works with type aliases. For
example, given the type definition for port from the base schema `type port =
count`, a search for `:count` will also consider fields of type `port`.
