---
title: Compactor no longer abandons continuation handlers spuriously
type: bugfix
authors:
  - IyeOnline
prs:
  - 6470
created: 2026-07-23T14:35:31.329568Z
---

The compactor no longer logs spurious "*compactor abandons continuation handler
for run N because it is not the current run anymore*" messages during temporal
and spatial compaction runs.

These log messages were harmless overall, caused by a failure during compaction
which did not properly stop the compaction run.
