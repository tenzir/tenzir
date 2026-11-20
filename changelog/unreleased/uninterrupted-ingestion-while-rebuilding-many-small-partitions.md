---
title: Uninterrupted ingestion while rebuilding many small partitions
type: bugfix
authors:
  - tobim
created: 2026-11-20T15:09:19.000000Z
---

Ingestion no longer stalls while a rebuild merges many small partitions, or while the `partitions`, `schemas`, or `fields` operators run on a large database.

Previously, every rebuild batch over thousands of small partitions paused the node's partition bookkeeping for up to a minute, and incoming events had to wait until it finished. Pipelines that ran `partitions` periodically, for example to export storage metrics, caused a similar pause of several seconds each time.
