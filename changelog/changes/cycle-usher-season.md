---
title: "Improvements to `context::enrich`"
type: change
authors: jachris
pr: 5388
---

The `context::enrich` operator now allows using `mode="append"` even if the
enrichment does not have the exact same type as the existing type, as long as
they are compatible. Furthermore, `mode="ocsf"` now returns `null` if no
enrichment took place instead of a record with a `null` data field.
