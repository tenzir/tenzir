---
title: More reliable storage maintenance
type: change
authors:
  - tobim
created: 2026-08-27T14:20:00.000000Z
---

Storage maintenance now handles changes during rebuilds and compaction more
reliably. Disk-budget eviction uses the age of the data rather than the age of
its files, and no longer removes data needed by ongoing work. Automatic
rebuilds now group data by day and run hourly.
