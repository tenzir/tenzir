---
title: Platform websocket proxy support
type: feature
authors:
  - tobim
  - codex
pr: 6039
created: 2026-04-16T13:50:51.852936Z
---

Tenzir nodes now honor standard HTTP proxy environment variables when connecting to Tenzir Platform:

```sh
HTTPS_PROXY=http://proxy.example:3128 tenzir-node
```

Use `NO_PROXY` to bypass the proxy for selected hosts. This helps deployments where outbound connections to the Platform websocket gateway must go through an HTTP proxy.
