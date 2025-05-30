---
title: "Fix a segfault in `save_amqp` on connection loss"
type: bugfix
authors: IyeOnline
pr: 5226
---

We fixed a crash in `save_amqp` when trying to send a message after the connection
was lost.
