---
title: "PRs 4295-4322-4325"
type: bugfix
authors: tobim
pr: 4295
---

We fixed a bug that very rarely caused configured pipelines using contexts to
fail starting up because the used context was not available, and similarly to
fail shutting down because the used context was no longer available before the
pipeline was shut down.
