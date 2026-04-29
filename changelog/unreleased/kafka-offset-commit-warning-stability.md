---
title: Kafka offset commit warning stability
type: bugfix
authors:
  - mavam
  - codex
pr: 6090
created: 2026-04-29T12:40:55.805121Z
---

`from_kafka` no longer emits spurious offset commit warnings for transient Kafka group coordinator errors, such as `Broker: Not coordinator`, when the broker is still settling during startup or shutdown.
