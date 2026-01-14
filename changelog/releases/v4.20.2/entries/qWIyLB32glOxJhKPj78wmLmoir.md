---
title: "Set `SO_REUSEADDR` in the UDP connector"
type: bugfix
author: dominiklohmann
created: 2024-09-06T12:28:29Z
pr: 4579
---

Restarting pipelines with the `udp` connector no longer fails to bind to the
socket.
