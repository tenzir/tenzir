---
title: Event throughput metrics for the new executor
type: feature
authors:
  - mavam
  - codex
created: 2026-05-12T10:26:00.599424Z
---

Pipeline metrics now report event throughput alongside byte throughput for
pipelines running on the new executor:

```tql
metrics "pipeline"
summarize ingress_events=sum(ingress.events), ingress_bytes=sum(ingress.bytes), egress_events=sum(egress.events), pipeline_id
sort -egress_events
```

This makes node metrics distinguish the amount of data transferred from the
number of events processed.
