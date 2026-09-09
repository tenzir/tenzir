---
title: Google Cloud Pub/Sub sources stay running
type: bugfix
authors:
  - raxyte
created: 2026-09-09T09:23:35.840071Z
---

Pipelines using `from_google_cloud_pubsub` no longer complete immediately after startup. The operator now keeps its subscription connection alive to continue receiving messages.
