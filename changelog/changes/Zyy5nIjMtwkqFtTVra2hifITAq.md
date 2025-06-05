---
title: "Fix `export --live` and introduce `metrics`"
type: feature
authors: dominiklohmann
pr: 3790
---

The `metrics` operator returns internal metrics events generated in a Tenzir
node. Use `metrics --live` to get a feed of metrics as they are being generated.
