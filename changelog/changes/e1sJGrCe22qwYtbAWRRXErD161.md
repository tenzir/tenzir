---
title: "Support dropping entire schemas in `drop` operator"
type: feature
authors: dominiklohmann
pr: 2419
---

The `drop` pipeline operator now drops entire schemas spcefied by name in the
`schemas` configuration key in addition to dropping fields by extractors in the
`fields` configuration key.
