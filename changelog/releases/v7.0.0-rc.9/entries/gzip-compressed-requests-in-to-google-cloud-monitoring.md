---
title: Gzip-compressed requests in to_google_cloud_monitoring
type: change
authors:
  - jachris
created: 2026-11-25T20:27:00.35523Z
---

The `to_google_cloud_monitoring` operator now compresses its requests to the Cloud Monitoring API with gzip, which reduces the network traffic it causes. Set `compress=false` to send uncompressed requests instead.
