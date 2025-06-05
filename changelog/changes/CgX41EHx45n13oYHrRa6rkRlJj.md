---
title: "Implement Apache Parquet & Apache Feather V2 stores"
type: change
authors: dominiklohmann
pr: 2413
---

Metrics for VAST's store lookups now use the keys
`{active,passive}-store.lookup.{runtime,hits}`. The store type metadata field
now distinguishes between the various supported store types, e.g., `parquet`,
`feather`, or `segment-store`, rather than containing `active` or `passive`.
