---
title: Check Point syslog structured-data dialect parsing
type: feature
authors:
  - mavam
  - codex
pr: 5851
created: 2026-03-02T10:22:49.136178Z
---

`parse_syslog()` and `read_syslog` now accept common Check Point structured-data variants that are not strictly RFC 5424 compliant. This includes `key:"value"` parameters, semicolon-separated parameters, and records that omit an SD-ID entirely.

For records without an SD-ID, Tenzir now normalizes the structured data under `checkpoint_2620`, so downstream pipelines can use a stable field path.

For example, the message `<134>1 ... - [action:"Accept"; conn_direction:"Incoming"]` now parses successfully and maps to `structured_data.checkpoint_2620`. This improves interoperability with Check Point exports and reduces ingestion-time preprocessing.
