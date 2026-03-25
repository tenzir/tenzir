---
title: Fix batch timeout to flush asynchronously
type: bugfix
authors:
  - aljazerzen
pr: 5906
created: 2026-03-14T00:00:00Z
---

The batch timeout was only checked when a new event arrived, so a single event
followed by an idle stream would never be emitted. The timeout now fires
independently of upstream activity.
