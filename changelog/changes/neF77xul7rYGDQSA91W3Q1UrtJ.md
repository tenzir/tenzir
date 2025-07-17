---
title: "Add a `rendered` field to diagnostics"
type: feature
authors: dominiklohmann
pr: 4290
---

Newly created diagnostics returned from the `diagnostics` now contain a
`rendered` field that contains a rendered form of the diagnostic. To restore the
previous behavior, use `diagnostics | drop rendered`.
