---
title: Broader pipeline pushdown through transforms
type: change
authors:
  - aljazerzen
created: 2026-09-10T17:04:38.32919Z
---

Pipeline optimization now carries field selection and row limits through more common transformations, allowing sources and readers to materialize less data in pipelines that use field removal, batching, replacement, history, time shifting, enrichment, and OCSF processing.

Conservative barriers preserve event ordering, history, diagnostics, and whole-event observations where pushdown would change behavior. Enrichment and lookup operators pass filters and limits through, so optimization may skip context requests and DNS lookups for events that downstream discards.
