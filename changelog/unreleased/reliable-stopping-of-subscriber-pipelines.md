---
title: Reliable stopping of subscriber pipelines
type: bugfix
authors:
  - aljazerzen
created: 2026-09-23T11:27:01.199686Z
---

Stopping a pipeline that uses `subscribe` now drains events it has already accepted and finishes without waiting for publishers to stop. Previously, stopping an individual subscriber pipeline could hang indefinitely. Node shutdown still waits for publishers to flush their remaining events.
