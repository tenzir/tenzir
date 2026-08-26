---
title: Native Splunk HEC ingestion
type: feature
authors:
  - mavam
created: 2026-08-25T13:16:42.073902Z
---

Tenzir can now receive Splunk HTTP Event Collector (HEC) traffic without a Fluent Bit bridge. The new `accept_splunk` operator accepts event and raw requests, gzip compression, HEC token authentication, and TLS:

```tql
accept_splunk hec_token=secret("splunk-hec-token")
publish "splunk"
```

The listener defaults to the standard HEC port `8088`, preserves HEC metadata and raw request boundaries, and validates complete batches before emitting events.
