---
title: "Report by schema metrics from the importer"
type: feature
authors: tobim
pr: 2274
---

VAST now produces additional metrics under the keys `ingest.events`,
`ingest.duration` and `ingest.rate`. Each of those gets issued once for every
schema that VAST ingested during the measurement period. Use the
`metadata_schema` key to disambiguate the metrics.
