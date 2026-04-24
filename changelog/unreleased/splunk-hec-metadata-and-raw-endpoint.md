---
title: Splunk HEC metadata and raw endpoint support
type: feature
authors:
  - mavam
  - codex
created: 2026-04-24T07:39:02Z
---

The `to_splunk` operator now supports additional Splunk HTTP Event Collector
metadata and raw endpoint ingestion:

```tql
to_splunk "https://splunk:8088",
  hec_token=secret("SPLUNK_HEC_TOKEN"),
  endpoint="raw",
  event=message,
  sourcetype="syslog"
```

Use `time=...` to set Splunk `_time`, `fields={...}` to attach event-mode
indexed fields, and `endpoint="raw"` to send already-formatted raw log text to
`/services/collector/raw`.
