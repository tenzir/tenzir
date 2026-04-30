---
title: ClickHouse TLS mismatch diagnostics
type: bugfix
authors:
  - mavam
  - codex
pr: 6098
created: 2026-04-29T16:59:13.143667Z
---

ClickHouse connection errors caused by TLS/plaintext mismatches now include the TLS notes and hint again. This helps identify when `to_clickhouse` is using TLS against a plaintext ClickHouse endpoint and suggests setting `tls=false` when appropriate.
