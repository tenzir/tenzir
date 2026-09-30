---
title: Data loss with percentage-based disk budgets
type: bugfix
authors:
  - tobim
created: 2026-09-30T09:40:55Z
---

Custom disk-usage checks such as `tenzir-df-percent` once again preserve data when
usage is below the configured watermarks. A regression in Tenzir v6.19.0 treated
percentage thresholds as byte limits, which could delete existing data and newly
ingested events even with ample free disk space. Eviction now checks disk usage
again after each deletion batch. This fix cannot restore data already deleted.
