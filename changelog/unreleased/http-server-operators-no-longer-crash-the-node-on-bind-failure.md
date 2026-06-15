---
title: HTTP server operators no longer crash the node on bind failure
type: bugfix
authors:
  - Zedoraps
prs:
  - 6267
created: 2026-06-09T11:48:35.337776Z
---

The `accept_http`, `accept_opensearch`, and `serve_http` operators no longer
abort the node when they fail to bind their endpoint. Starting a second
pipeline on a port that is already in use—or restarting one before the
previous instance has released its socket—previously crashed the entire node
process. Now the operator emits a regular diagnostic:

```text
error: failed to start HTTP server: failed to bind to async server socket:
0.0.0.0:8774: Address already in use
```

The pipeline that hit the conflict exits with an error while every other
pipeline running on the node keeps going.
