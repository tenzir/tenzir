---
title: Compaction deletes old partitions without reading them
type: change
authors:
  - tobim
created: 2026-11-26T12:00:00.000000Z
---

A compaction rule with the pipeline `where false` now deletes a partition whose events are all older than the rule's cutoff without reading the partition first. Previously, such a rule loaded the entire partition into memory, only to drop every event in it. On nodes with large partitions, this could exhaust memory as soon as the node started.

Partitions that also hold events newer than the cutoff, or events without an import time, still run through the pipeline as before.
