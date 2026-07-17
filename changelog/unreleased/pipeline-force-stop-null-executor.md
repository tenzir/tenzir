---
title: Stopping a recovered pipeline no longer crashes the node
type: bugfix
authors:
  - aljazerzen
created: 2026-06-29T00:00:00Z
---

Fixes a node crash (`assertion 'pipeline.executor' failed`) that could occur
when stopping or force-stopping a pipeline that had no live executor.
