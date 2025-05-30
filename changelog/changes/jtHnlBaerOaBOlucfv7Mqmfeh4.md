---
title: "Implement the `lookup` operator"
type: feature
authors: Dakostu
pr: 3721
---

The new `lookup` operator performs live filtering of the import feed using a
context, and translates context updates into historical queries. This
effectively enables live and retro matching in a single operator.
