---
title: Filters reach the source through constant literals again
type: bugfix
created: 2026-11-05T15:00:00Z
---

Filters on OCSF fields reach the source again when they follow an OCSF mapping
that assigns constant lists or records, such as `metadata = {profiles:
["cloud"], version: "1.9.0"}`. Since v6.21.0, such filters stayed behind the
assignment, because the optimizer treated every read of a constant list or
record as a copy of computed work. Such literals now evaluate to a constant
without per-event work, so filters may read them any number of times.
