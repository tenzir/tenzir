---
title: "Support dropping entire schemas in `drop` operator"
type: feature
author: dominiklohmann
created: 2022-07-11T13:22:22Z
pr: 2419
---

The `drop` pipeline operator now drops entire schemas spcefied by name in the
`schemas` configuration key in addition to dropping fields by extractors in the
`fields` configuration key.
