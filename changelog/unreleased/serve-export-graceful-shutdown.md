---
title: Serve and live export shut down promptly
type: bugfix
authors:
  - aljazerzen
created: 2026-07-01T00:00:00.000000Z
---

Stopping a pipeline that ends in `serve` no longer waits for the entire buffer
to drain, which previously made graceful shutdown slow or made it hang
indefinitely when a large backlog was buffered with no client draining it. The
buffered data is now dropped and pending requests are released so the operator
exits immediately.

Live exports with `retro=true` no longer ignore graceful shutdown. Previously
the pipeline could enter an indefinite live-wait after the retrospective
backlog drained and never terminate.
