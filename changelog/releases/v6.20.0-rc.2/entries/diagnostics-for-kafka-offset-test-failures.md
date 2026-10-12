---
title: Diagnostics for Kafka offset test failures
type: change
authors:
  - aljazerzen
created: 2026-10-09T12:54:02.161837Z
---

Kafka offset-preservation test failures now include the failing case, expected and actual records, and diagnostics from both consumer runs. This makes intermittent CI failures actionable without changing Kafka consumption behavior.
