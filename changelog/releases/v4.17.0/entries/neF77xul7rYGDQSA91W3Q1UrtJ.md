---
title: "Add a `rendered` field to diagnostics"
type: feature
author: dominiklohmann
created: 2024-06-12T10:26:51Z
pr: 4290
---

Newly created diagnostics returned from the `diagnostics` now contain a
`rendered` field that contains a rendered form of the diagnostic. To restore the
previous behavior, use `diagnostics | drop rendered`.
