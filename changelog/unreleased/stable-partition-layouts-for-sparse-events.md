---
title: Stable partition layouts for sparse events
type: bugfix
authors:
  - IyeOnline
created: 2026-10-29T12:00:00.000000Z
---

Importing events whose schema has many optional fields, such as Suricata EVE
events, no longer creates a new partition layout for every combination of
fields that happen to be null. Fields that are null throughout a partition now
take their type from the registered schema of the events, so partitions of the
same schema share one layout again.
