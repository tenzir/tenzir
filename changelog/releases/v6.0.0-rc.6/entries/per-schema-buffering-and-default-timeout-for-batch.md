---
title: Per-schema buffering and default timeout for `batch`
type: change
author: aljazerzen
prs:
  - 5878
  - 5906
created: 2026-04-30T13:01:26.71885Z
---

The `batch` operator now maintains separate buffers for each distinct
schema. Each buffer has independent timeout tracking and fills until reaching
the `limit`, at which point it flushes immediately. Previously, mixed-schema
streams could stall waiting for a single combined buffer to fill.

The `timeout` argument now defaults to `1min` instead of an infinite
duration, so buffered events are flushed at least once per minute when no new
events arrive.
