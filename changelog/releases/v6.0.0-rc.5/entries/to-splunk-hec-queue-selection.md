---
title: HEC queue selection in `to_splunk`
type: feature
authors:
  - mavam
  - codex
created: 2026-05-18T00:00:00Z
---

The neo `to_splunk` implementation now accepts `queue="indexing"` and
`queue="typing"` for selecting the Splunk HEC processing queue. The default
`indexing` path keeps Splunk's regular HEC behavior, while `typing` sends the
Splunk `parsingQueue` hint in HEC event envelopes for receivers that support
this non-standard HEC metadata.

The default is `queue="indexing"`. The `typing` queue is rejected with
`raw=...`, because Splunk's raw HEC endpoint sends raw requests to the indexer
queue.
