---
title: "Fix a segfault in `save_amqp` on connection loss"
type: bugfix
author: IyeOnline
created: 2025-05-26T19:04:59Z
pr: 5226
---

We fixed a crash in `save_amqp` when trying to send a message after the connection
was lost.
