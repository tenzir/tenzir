---
title: "Add a `hidden` field to `diagnostics`"
type: feature
authors: dominiklohmann
pr: 5119
---

The output of the `diagnostics` operator now includes an additional `hidden`
field that is set to `true` for pipelines that are not visible on the Pipelines
page of the Tenzir Platform, e.g., because they're run under-the-hood by the
platform or interactively in the Explorer.
