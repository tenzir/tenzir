---
title: Parallelize `to_google_secops`
type: feature
authors:
  - aljazerzen
---

The `to_google_secops` operator now runs as multiple instances when the
pipeline is parallelized. Every instance batches, authenticates, and sends
independently, so `parallel` bounds the in-flight requests per instance.
