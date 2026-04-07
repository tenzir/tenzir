---
title: Reliable export for null rows in rebuilt partitions
type: bugfix
authors:
  - tobim
  - codex
pr: 5988
created: 2026-04-07T11:40:00.752835Z
---

The `export` operator no longer emits partially populated events from rebuilt partitions when a row is null at the record level. Previously, some events could appear with most fields set to `null` while a few values, such as `event_type` or interface fields, were still present.

This makes exports from rebuilt data more reliable when investigating sparse or malformed-looking events.
