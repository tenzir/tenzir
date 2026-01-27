---
title: "Implement `context load`, `context save`, and `context reset`"
type: feature
author: eliaskosunen
created: 2024-02-13T13:08:39Z
pr: 3908
---

The `context reset` operator allows for clearing the state of a context.

The `context save` and `context load` operators allow serializing and
deserializing the state of a context to/from bytes.
