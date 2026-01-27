---
title: "Implement `head` and `taste` operators"
type: feature
author: dominiklohmann
created: 2023-01-30T15:15:01Z
pr: 2891
---

The new `head` and `taste` operators limit results to the specified number of
events. The `head` operator applies this limit for all events, and the `taste`
operator applies it per schema. Both operators take the limit as an optional
argument, with the default value being 10.
