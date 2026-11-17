---
title: Platform connection drops behind load balancers and under load
type: bugfix
authors:
  - tobim
created: 2026-11-17T09:50:57.532614Z
---

Nodes no longer lose their connection to the Tenzir Platform every few minutes
when a load balancer or proxy with an idle timeout sits between the two. The
node now sends a WebSocket ping every 20 seconds. Previously, it could stay
silent for a full minute, so a load balancer with a 60-second idle timeout
dropped the connection again and again, and the node logged `platform plugin
received an unexpected error: End of file`.

Nodes also keep the connection while they answer many requests at once, for
example when a dashboard loads. Previously, this could corrupt the encrypted
connection. The node then logged `unexpected error: unspecified system error`
or `stream truncated` and briefly appeared as disconnected in the app.
