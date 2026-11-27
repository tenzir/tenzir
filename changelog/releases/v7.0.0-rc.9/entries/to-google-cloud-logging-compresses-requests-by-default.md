---
title: to_google_cloud_logging compresses requests by default
type: change
authors:
  - jachris
created: 2026-11-25T18:27:52.333281Z
---

The `to_google_cloud_logging` operator now compresses its write requests to Google Cloud Logging with gzip by default. Cloud Logging decompresses the requests transparently, so this reduces network egress without changing the ingested log entries.

Use the new `compress=false` option to send uncompressed requests, for example when CPU usage matters more than network traffic.
