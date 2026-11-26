---
title: Compressed Google Cloud Pub/Sub publish requests
type: change
authors:
  - jachris
created: 2026-11-25T20:26:19.461552Z
---

The `to_google_cloud_pubsub` operator now compresses publish requests with gzip by default, which reduces the network traffic sent to Google Cloud Pub/Sub. The operator compresses requests of at least 240 bytes and sends smaller requests uncompressed, because compression does not pay off for them. Pub/Sub decompresses the requests transparently, so subscribers receive the original message data.

Use `compress=false` to restore the previous behavior:

```tql
to_google_cloud_pubsub project_id="my-project", topic_id="alerts",
  message=text, compress=false
```
