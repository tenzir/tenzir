---
title: Send events to webhooks with `to_http`
type: feature
author: aljazerzen
pr: 6019
created: 2026-04-30T12:59:46.103696Z
---

The new `to_http` operator sends each input event as an HTTP request to a
webhook or API endpoint. By default, it JSON-encodes the entire event as the
request body and sends it as a `POST`:

```tql
subscribe "alerts"
to_http "https://example.com/webhook"
```

`to_http` shares its options with `from_http` and `http`: configure
`method`, `body`, `encode`, `headers`, TLS, retries, and pagination per
request. Use `parallel` to issue multiple concurrent requests when the target
endpoint can keep up with a single pipeline.

This is useful for pushing alerts to webhooks, forwarding events to SIEMs,
and calling external APIs once per event.
