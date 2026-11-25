---
title: Add timestamp, label, cache, and variable options to Axiom operators
type: feature
authors:
  - mavam
created: 2026-11-22T19:09:54.131128Z
---

`to_axiom` now accepts `timestamp_field`, `timestamp_format`, and `labels` to control how Axiom extracts `_time` (including dotted field paths and custom Go reference time layouts) and attaches event labels via `X-Axiom-Event-Labels`.

`from_axiom` now accepts `nocache` to bypass Axiom's query cache and `variables` to pass typed parameters into `query=` APL queries. For example:

```tql
from_file "ocsf-events.ndjson"
to_axiom "security-events",
  token=secret("axiom_token"),
  timestamp_field="time",
  labels={env: "prod"}
```
