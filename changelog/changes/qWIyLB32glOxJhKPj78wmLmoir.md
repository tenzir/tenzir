---
title: "Set `SO_REUSEADDR` in the UDP connector"
type: bugfix
authors: dominiklohmann
pr: 4579
---

Restarting pipelines with the `udp` connector no longer fails to bind to the
socket.
