---
title: "Enable type extractors to support user defined types"
type: feature
authors: tobim
pr: 1382
---

The type extractor in the expression language now works with user defined types.
For example the type `port` is defined as `type port = count` in the base
schema. This type can now be queried with an expression like `:port == 80`.
