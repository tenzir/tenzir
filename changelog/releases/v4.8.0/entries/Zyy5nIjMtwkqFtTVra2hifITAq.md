---
title: "Fix `export --live` and introduce `metrics`"
type: feature
author: dominiklohmann
created: 2024-01-11T19:08:26Z
pr: 3790
---

The `metrics` operator returns internal metrics events generated in a Tenzir
node. Use `metrics --live` to get a feed of metrics as they are being generated.
