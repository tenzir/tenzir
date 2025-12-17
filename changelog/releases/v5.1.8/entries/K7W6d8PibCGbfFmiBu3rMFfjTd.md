---
title: "Make amqp saver/loader detached"
type: bugfix
author: IyeOnline
created: 2025-05-20T12:13:14Z
pr: 5206
---

We fixed a bug in `save_amqp` that caused the operator to not send any messages.
