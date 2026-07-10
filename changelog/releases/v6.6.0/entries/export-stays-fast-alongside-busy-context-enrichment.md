---
title: Export stays fast alongside busy context enrichment
type: bugfix
authors:
  - IyeOnline
created: 2026-07-03T20:07:17.066853Z
prs:
  - 6420
---

The `export` operator no longer stalls when a busy `context` enrichment pipeline (such as one using `lookup`) is reading from disk at the same time. Both used to compete for the same disk access, and a busy enrichment pipeline could make `export` dramatically slower or effectively stuck. `export` now takes priority over background enrichment reads, so it stays responsive regardless of how much enrichment work is running concurrently.
