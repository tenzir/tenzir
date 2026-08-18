---
title: Bounded buffering and coordinated retries for Google SecOps
type: bugfix
authors:
  - raxyte
created: 2026-08-11T12:55:16.287282Z
---

The `to_google_secops` operator now bounds buffered request batches according
to `parallel` and propagates backpressure upstream when all request slots are
occupied.

Requests that receive a `429` or `5XX` response now share one retry cooldown.
After the cooldown, the operator sends one probe request at a time, honors
`Retry-After`, and resumes queued requests when a request succeeds. Retryable
responses no longer cause request batches to be discarded after a fixed number
of attempts.
